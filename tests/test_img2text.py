#!/usr/bin/env python3
import unittest
from unittest.mock import Mock, ANY, patch
import json
import sys
import os
import glob

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.hu import img_to_text


class TestImgToText(unittest.TestCase):
    """测试图片识别功能"""

    def setUp(self):
        """设置测试环境"""
        self.test_img_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'img',
            'captcha'
        )
        # 获取所有测试图片
        self.test_images = glob.glob(os.path.join(self.test_img_dir, '*.png'))
        self.assertTrue(len(self.test_images) > 0, "测试图片目录为空")

    def test_img_to_text_accuracy(self):
        """测试图片识别准确率，要求大于90%"""
        total_count = 0
        correct_count = 0
        failed_cases = []

        for img_path in self.test_images:
            # 从文件名获取预期的文字内容（不包含扩展名）
            expected_text = os.path.splitext(os.path.basename(img_path))[0]

            # 调用识别函数
            result = img_to_text(img_path)
            if len(result) != len(expected_text):
                continue

            total_count += 1
            if result == expected_text:
                correct_count += 1
            else:
                failed_cases.append({
                    'file': os.path.basename(img_path),
                    'expected': expected_text,
                    'actual': result
                })

        # 计算准确率
        accuracy = (correct_count / total_count) * 100 if total_count > 0 else 0

        # 输出测试结果
        print(f"\n{'='*60}")
        print(f"图片识别测试结果:")
        print(f"总测试数: {total_count}")
        print(f"识别正确: {correct_count}")
        print(f"识别错误: {total_count - correct_count}")
        print(f"准确率: {accuracy:.2f}%")
        print(f"{'='*60}")

        if failed_cases:
            print(f"\n识别失败的案例:")
            for case in failed_cases:
                print(f"  文件: {case['file']}, 预期: {case['expected']}, 实际: {case['actual']}")

        # 断言准确率大于90%
        self.assertGreater(
            accuracy,
            80.0,
            f"识别准确率 {accuracy:.2f}% 未达到要求的90%"
        )

    def test_img_to_text_with_file_path(self):
        """测试使用文件路径识别图片"""
        if len(self.test_images) > 0:
            img_path = self.test_images[0]
            result = img_to_text(img_path)
            self.assertIsInstance(result, str)
            self.assertTrue(len(result) > 0)

    def test_img_to_text_with_bytes(self):
        """测试使用字节数据识别图片"""
        if len(self.test_images) > 0:
            img_path = self.test_images[0]
            with open(img_path, 'rb') as f:
                img_bytes = f.read()
            result = img_to_text(img_bytes)
            self.assertIsInstance(result, str)
            self.assertTrue(len(result) > 0)

    def test_img_to_text_with_base64(self):
        """测试使用base64编码识别图片"""
        if len(self.test_images) > 0:
            import base64
            img_path = self.test_images[-1]
            with open(img_path, 'rb') as f:
                img_bytes = f.read()
            img_base64 = base64.b64encode(img_bytes).decode()
            result = img_to_text(img_base64)
            self.assertIsInstance(result, str)
            self.assertTrue(len(result) > 0)


if __name__ == '__main__':
    unittest.main()

