import h5py
import numpy as np
import shutil
import os
import sys

file_path = '/Users/chuazhou/dev/pylone/data/trans_300.h5'
backup_path = file_path + '.backup2'
new_file_path = file_path + '.new'
report_path = file_path + '.report.txt'

def full_integrity_check(dset):
    """Full chunk-by-chunk integrity check, returns (is_ok, corrupted_chunks_count)"""
    total = len(dset)
    chunk_size = dset.chunks[0] if dset.chunks else 10000
    
    corrupted = 0
    for start in range(0, total, chunk_size):
        end = min(start + chunk_size, total)
        try:
            _ = dset[start:end]
        except:
            corrupted += 1
    return corrupted == 0, corrupted


def recover_all_good_data(dset):
    """Recover all readable data, even if corruption is in the middle"""
    total = len(dset)
    chunk_size = dset.chunks[0] if dset.chunks else 10000
    
    good_chunks = []
    corrupted_count = 0
    
    for start in range(0, total, chunk_size):
        end = min(start + chunk_size, total)
        try:
            chunk = dset[start:end]
            good_chunks.append(chunk)
        except:
            corrupted_count += 1
            continue  # Skip corrupted chunk, keep going
    
    if not good_chunks:
        return None, total, 0
    
    good_data = np.concatenate(good_chunks)
    removed = total - len(good_data)
    return good_data, removed, corrupted_count

print("=" * 80)
print("HDF5 Repair Tool - Auto-detect and fix corrupted datasets")
print("=" * 80)
print(flush=True)

# Step 1: Create backup
print("Step 1: Creating backup...")
if not os.path.exists(backup_path):
    shutil.copy2(file_path, backup_path)
    print(f"  Backup: {backup_path}", flush=True)
else:
    print(f"  Backup already exists: {backup_path}", flush=True)

# Step 2: Quick scan all datasets
print(flush=True)
print("Step 2: Quick scanning datasets...", flush=True)

dataset_status = {}
need_recovery = []

with h5py.File(backup_path, 'r') as f:
    grp = f['data']
    total = len(grp.keys())
    
    for idx, name in enumerate(grp.keys()):
        sys.stdout.write(f"\r  Scanning {idx+1}/{total}: {name}...")
        sys.stdout.flush()
        
        dset = grp[name]
        total_records = len(dset)
        
        # Full integrity check (checks ALL chunks, not just first/last)
        is_ok, corrupted_chunks = full_integrity_check(dset)
        
        if is_ok:
            dataset_status[name] = {'status': 'ok', 'records': total_records}
        else:
            need_recovery.append((name, 'corrupted', total_records, corrupted_chunks))

print(f"\r{' ' * 80}", flush=True)
print(f"\r  Found {len(need_recovery)} datasets needing recovery", flush=True)

# Step 3: Recover problematic datasets
print(flush=True)
print("Step 3: Recovering problematic datasets...", flush=True)

for name, reason, total_records, corrupted_chunks in need_recovery:
    print(f"  {name} ({reason}, {corrupted_chunks} bad chunks)...", end='', flush=True)
    
    with h5py.File(backup_path, 'r') as f:
        dset = f['data'][name]
        good_data, removed, chunks_skipped = recover_all_good_data(dset)
        
        if good_data is not None and len(good_data) > 0:
            dataset_status[name] = {
                'status': 'repaired',
                'original': total_records,
                'kept': len(good_data),
                'removed': removed,
                'data': good_data,
                'compression': dset.compression if hasattr(dset, 'compression') else 'gzip'
            }
            print(f" OK ({len(good_data)}/{total_records}, removed {removed})", flush=True)
        else:
            dataset_status[name] = {
                'status': 'failed',
                'original': total_records,
                'kept': 0,
                'removed': total_records,
                'data': None,
                'compression': 'gzip'
            }
            print(f" FAILED", flush=True)

print(flush=True)

# Step 4: Create new file
print("Step 4: Creating repaired file...", flush=True)

with h5py.File(backup_path, 'r') as f_old:
    with h5py.File(new_file_path, 'w') as f_new:
        grp_old = f_old['data']
        grp_new = f_new.create_group('data')
        
        count = 0
        total = len(dataset_status)
        
        for name, info in dataset_status.items():
            count += 1
            sys.stdout.write(f"\r  Writing {count}/{total}: {name}...")
            sys.stdout.flush()
            
            if info.get('data') is not None:
                # Write recovered data
                grp_new.create_dataset(
                    name,
                    data=info['data'],
                    maxshape=(None,),
                    compression=info.get('compression', 'gzip'),
                    compression_opts=9
                )
            else:
                # Copy directly
                try:
                    grp_old.copy(name, grp_new)
                except Exception as e:
                    print(f"\n  Error copying {name}: {e}", flush=True)

print(f"\r{' ' * 80}", flush=True)
print(f"\r  New file: {new_file_path}", flush=True)
print(flush=True)

# Step 5: Generate report
print("Step 5: Generating report...", flush=True)

ok_count = sum(1 for v in dataset_status.values() if v.get('status') == 'ok')
repaired_count = sum(1 for v in dataset_status.values() if v.get('status') == 'repaired')
failed_count = sum(1 for v in dataset_status.values() if v.get('status') == 'failed')

with open(report_path, 'w') as f:
    f.write("=" * 80 + "\n")
    f.write("HDF5 Repair Report\n")
    f.write("=" * 80 + "\n\n")
    
    f.write(f"Original file: {file_path}\n")
    f.write(f"Backup file: {backup_path}\n")
    f.write(f"Repaired file: {new_file_path}\n")
    
    f.write(f"Total datasets: {len(dataset_status)}\n")
    f.write(f"OK: {ok_count}\n")
    f.write(f"Repaired: {repaired_count}\n")
    f.write(f"Failed: {failed_count}\n\n")
    
    if repaired_count > 0:
        f.write("-" * 80 + "\n")
        f.write("REPAIRED DATASETS:\n")
        f.write("-" * 80 + "\n")
        for name, info in sorted(dataset_status.items()):
            if info.get('status') == 'repaired':
                f.write(f"  {name}: {info.get('kept', 0)}/{info.get('original', 0)} records\n")
        f.write("\n")
    
    f.write("-" * 80 + "\n")
    f.write("ALL DATASETS:\n")
    f.write("-" * 80 + "\n")
    for name, info in sorted(dataset_status.items()):
        status = info.get('status', 'unknown')
        kept = info.get('kept', info.get('records', 0))
        original = info.get('original', info.get('records', 0))
        f.write(f"  {name}: {kept}/{original} ({status})\n")

print(f"  Report: {report_path}", flush=True)
print(flush=True)

# Summary
print("=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Original file: {file_path}")
print(f"Backup: {backup_path}")
print(f"Repaired file: {new_file_path}")
print(f"Report: {report_path}")
print(flush=True)
print(f"Total: {len(dataset_status)} datasets")
print(f"  OK: {ok_count}")
print(f"  Repaired: {repaired_count}")
print(f"  Failed: {failed_count}")
print(flush=True)
print(f"To replace original with repaired file:")
print(f"  mv {new_file_path} {file_path}")
print("=" * 80)
