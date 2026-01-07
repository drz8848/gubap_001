import asyncio
import random
import time
from typing import List, Tuple, Optional

import cv2
import numpy as np
from PIL import Image
from playwright.async_api import Page, Locator, Error as PlaywrightError

# 配置项（可根据实际验证码调整）
class SliderConfig:
    # 滑块滑动的基础速度（像素/毫秒）
    BASE_SPEED = 0.2
    # 轨迹波动系数（模拟人类手动滑动的不匀速）
    FLUCTUATION = 0.1
    # 滑动前的随机延迟（毫秒）
    PRE_DELAY_RANGE = (500, 1500)
    # 滑动后的停留时间（毫秒）
    POST_STAY_RANGE = (800, 1200)

class AsyncSliderCaptchaAgent:
    """异步滑块验证码智能体：处理各类网页滑块验证码"""
    
    def __init__(self, page: Page):
        """
        初始化智能体
        :param page: playwright的异步Page对象（来自异步浏览器操作）
        """
        self.page = page
        self.config = SliderConfig()

    async def _get_slider_elements(self, 
                                 slider_selector: str = "#slider",
                                 bg_selector: str = "#captcha-bg",
                                 target_selector: str = "#captcha-target") -> Tuple[Optional[Locator], Optional[Locator], Optional[Locator]]:
        """
        异步获取验证码的核心元素：滑块、背景图、目标缺口
        :param slider_selector: 滑块元素的CSS选择器
        :param bg_selector: 验证码背景图选择器
        :param target_selector: 缺口目标选择器
        :return: 滑块、背景、缺口元素的Locator对象
        """
        try:
            slider = self.page.locator(slider_selector)
            bg = self.page.locator(bg_selector)
            target = self.page.locator(target_selector)
            
            # 等待元素加载完成（异步等待）
            await asyncio.gather(
                slider.wait_for(state="visible", timeout=5000),
                bg.wait_for(state="visible", timeout=5000)
            )
            return slider, bg, target
        except PlaywrightError as e:
            print(f"获取验证码元素失败：{e}")
            return None, None, None

    async def _calculate_slide_distance(self, bg_image: Image.Image, target_image: Image.Image) -> int:
        """
        异步计算滑块需要滑动的距离（基于图像识别）
        :param bg_image: 验证码背景图
        :param target_image: 缺口目标图
        :return: 滑动距离（像素）
        """
        # 转换为OpenCV格式
        bg_cv = cv2.cvtColor(np.array(bg_image), cv2.COLOR_RGB2BGR)
        target_cv = cv2.cvtColor(np.array(target_image), cv2.COLOR_RGB2BGR)
        
        # 模板匹配（找缺口位置）
        result = cv2.matchTemplate(bg_cv, target_cv, cv2.TM_CCOEFF_NORMED)
        _, max_val, _, max_loc = cv2.minMaxLoc(result)
        
        # 过滤匹配结果（避免误识别）
        if max_val < 0.8:
            raise ValueError(f"缺口匹配度不足：{max_val}，无法确定滑动距离")
        
        # 返回缺口的X坐标（即需要滑动的距离）
        return max_loc[0]

    def _generate_human_trajectory(self, distance: int) -> List[Tuple[int, int, float]]:
        """
        生成模拟人类的滑动轨迹（非匀速、有波动）
        :param distance: 总滑动距离
        :return: 轨迹列表，每个元素为 (x偏移, y偏移, 停留时间)
        """
        trajectory = []
        current_x = 0
        
        # 分段生成轨迹：加速→匀速→减速（模拟人类操作）
        # 1. 加速阶段（前30%距离）
        accelerate_distance = int(distance * 0.3)
        while current_x < accelerate_distance:
            step = int(random.uniform(2, 5))
            current_x += step
            # 加速阶段速度越来越快，停留时间越来越短
            stay_time = random.uniform(0.01, 0.03)
            trajectory.append((step, random.randint(-1, 1), stay_time))
        
        # 2. 匀速阶段（中间50%距离）
        uniform_distance = int(distance * 0.5)
        while current_x < accelerate_distance + uniform_distance:
            step = int(random.uniform(3, 6))
            current_x += step
            stay_time = random.uniform(0.005, 0.015)
            trajectory.append((step, random.randint(-1, 1), stay_time))
        
        # 3. 减速阶段（最后20%距离）
        while current_x < distance:
            step = int(random.uniform(1, 3))
            # 避免超过目标距离
            if current_x + step > distance:
                step = distance - current_x
            current_x += step
            stay_time = random.uniform(0.02, 0.04)
            trajectory.append((step, random.randint(-1, 1), stay_time))
        
        return trajectory

    async def _slide_slider(self, slider: Locator, distance: int) -> bool:
        """
        异步执行滑块滑动（模拟人类操作）
        :param slider: 滑块元素Locator
        :param distance: 滑动距离
        :return: 是否滑动成功
        """
        try:
            # 获取滑块的位置和尺寸
            slider_bounding = await slider.bounding_box()
            if not slider_bounding:
                raise ValueError("无法获取滑块的位置信息")
            
            # 计算滑块的点击起始点（滑块中心）
            start_x = slider_bounding["x"] + slider_bounding["width"] / 2
            start_y = slider_bounding["y"] + slider_bounding["height"] / 2
            
            # 滑动前随机延迟（模拟人类思考时间）
            pre_delay = random.randint(*self.config.PRE_DELAY_RANGE) / 1000
            await asyncio.sleep(pre_delay)
            
            # 开始滑动：按下鼠标左键
            await self.page.mouse.move(start_x, start_y)
            await self.page.mouse.down()
            
            # 按轨迹逐步滑动
            trajectory = self._generate_human_trajectory(distance)
            for dx, dy, stay_time in trajectory:
                start_x += dx
                start_y += dy
                await self.page.mouse.move(start_x, start_y)
                await asyncio.sleep(stay_time)
            
            # 滑动后停留（模拟人类松开前的停顿）
            post_stay = random.randint(*self.config.POST_STAY_RANGE) / 1000
            await asyncio.sleep(post_stay)
            
            # 松开鼠标左键
            await self.page.mouse.up()
            
            # 等待验证结果（2秒）
            await asyncio.sleep(2)
            return True
        
        except PlaywrightError as e:
            print(f"滑块滑动执行失败：{e}")
            return False

    async def solve_slider_captcha(self, 
                                 slider_selector: str = "#slider",
                                 bg_selector: str = "#captcha-bg",
                                 target_selector: str = "#captcha-target",
                                 max_retry: int = 3) -> bool:
        """
        对外暴露的核心方法：异步解决滑块验证码
        :param slider_selector: 滑块选择器
        :param bg_selector: 背景图选择器
        :param target_selector: 缺口选择器
        :param max_retry: 最大重试次数
        :return: 是否验证成功
        """
        retry_count = 0
        while retry_count < max_retry:
            try:
                print(f"开始第 {retry_count + 1} 次验证滑块验证码...")
                
                # 1. 获取验证码元素
                slider, bg, target = await self._get_slider_elements(
                    slider_selector, bg_selector, target_selector
                )
                if not slider or not bg:
                    retry_count += 1
                    await asyncio.sleep(1)
                    continue
                
                # 2. 截取背景图和缺口图（异步截图）
                bg_screenshot = await bg.screenshot()
                target_screenshot = await target.screenshot()
                
                # 3. 转换为PIL图像
                bg_image = Image.open(bg_screenshot)
                target_image = Image.open(target_screenshot)
                
                # 4. 计算滑动距离
                slide_distance = await self._calculate_slide_distance(bg_image, target_image)
                print(f"计算出需要滑动的距离：{slide_distance} 像素")
                
                # 5. 执行滑块滑动
                slide_success = await self._slide_slider(slider, slide_distance)
                if not slide_success:
                    retry_count += 1
                    await asyncio.sleep(1)
                    continue
                
                # 6. 验证是否成功（可根据实际网页的成功标识调整）
                success_indicator = await self.page.locator(".captcha-success").is_visible(timeout=3000)
                if success_indicator:
                    print("滑块验证码验证成功！")
                    return True
                else:
                    print("验证码验证失败，准备重试...")
                    retry_count += 1
                    await asyncio.sleep(1)
            
            except Exception as e:
                print(f"第 {retry_count + 1} 次验证出错：{e}")
                retry_count += 1
                await asyncio.sleep(1)
        
        print(f"已重试 {max_retry} 次，滑块验证码验证失败")
        return False

# ------------------- 测试用例（可单独运行验证） -------------------
async def test_captcha_solver():
    """测试异步滑块验证码智能体"""
    from playwright.async_api import async_playwright
    
    async with async_playwright() as p:
        # 启动浏览器（无头模式可改为False）
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        
        # 访问测试页面（替换为实际验证码页面）
        await page.goto("https://example.com/captcha-page")
        
        # 初始化智能体
        captcha_agent = AsyncSliderCaptchaAgent(page)
        
        # 解决验证码
        success = await captcha_agent.solve_slider_captcha(
            slider_selector="#slider-btn",
            bg_selector="#captcha-background",
            target_selector="#captcha-gap"
        )
        
        print(f"测试结果：{'验证成功' if success else '验证失败'}")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(test_captcha_solver())