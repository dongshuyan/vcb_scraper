#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VCB-S 网站爬虫
功能：自动爬取 vcb-s.com 网站的帖子信息并提取磁链
"""

import requests
from bs4 import BeautifulSoup
import re
import time
import json
import sqlite3
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import argparse
from datetime import datetime
from urllib.parse import urlparse, urljoin
import logging
from typing import Dict, List, Optional, Tuple
import traceback
import random
import os
import shutil

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class VCBScraper:
    """VCB-S 网站爬虫类"""
    
    def __init__(self, max_pages: int = 100, workers: int = 4, window_size: int = 50, update_mode: bool = False):
        """
        初始化爬虫
        :param max_pages: 最大爬取页数
        :param workers: 并行线程数
        :param window_size: 滑动窗口大小（每次处理的帖子数量）
        :param update_mode: 是否为更新模式（自动从第1页开始更新）
        """
        self.max_pages = max_pages
        self.workers = workers
        self.window_size = window_size
        self.update_mode = update_mode
        self.base_url = "https://vcb-s.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # 输出目录与文件路径
        self.base_output_dir = os.getcwd()
        self.run_dir_regex = re.compile(r'^\d{2}-\d{2}-\d{2}-\d{2}-\d{2}$')
        self.historic_run_dirs = self._collect_existing_run_dirs()
        self.previous_run_dir = self.historic_run_dirs[-1] if self.historic_run_dirs else None
        self.output_dir = self._create_output_dir()
        self.log_file = os.path.join(self.output_dir, 'vcb_scraper.log')
        self.db_path = os.path.join(self.output_dir, 'vcb_data.db')
        self.excel_path = os.path.join(self.output_dir, 'vcb_data.xlsx')
        self.page_metadata: Dict[str, Dict] = {}
        self.excluded_tags = {"我们爱科普", "计划与日志"}
        
        # 数据库和文件锁
        self.db_lock = Lock()
        self.excel_lock = Lock()
        
        # 风控应对配置
        self.max_retries = 10  # 最大重试次数
        self.base_sleep_time = 2  # 基础睡眠时间（秒）
        self.max_retry_sleep = 60  # 重试时的最大延迟（秒）
        self.request_count = 0  # 请求计数
        self.request_lock = Lock()
        
        # 断点续抓配置
        self.checkpoint_file = os.path.join(self.output_dir, 'scraper_checkpoint.json')
        self.checkpoint_lock = Lock()
        self.failure_log_file = os.path.join(self.output_dir, 'scraper_failures.json')
        self.failure_log_lock = Lock()
        self.previous_failure_file = self._find_latest_artifact('scraper_failures.json')
        
        # 运行期统计
        self.failure_lock = Lock()
        self.failed_posts = set()
        self.posts_with_failed_magnets = set()
        self.magnet_success_records = []
        self.magnet_failure_records = []
        
        # 运行目录准备
        self._bootstrap_from_previous_run()
        self._configure_file_logger()
        
        # 初始化数据库
        self._init_database()
        
        # 磁链域名优先级
        self.magnet_domains = [
            'nyaa.si',
            'share.dmhy.org',
            'bangumi.moe'
        ]

    def _collect_existing_run_dirs(self) -> List[str]:
        """收集历史运行目录并按时间排序"""
        candidates = []
        try:
            for entry in os.listdir(self.base_output_dir):
                full_path = os.path.join(self.base_output_dir, entry)
                if os.path.isdir(full_path) and self.run_dir_regex.match(entry):
                    candidates.append(full_path)
        except Exception as e:
            logger.error(f"扫描历史运行目录失败: {e}")
        candidates.sort()
        return candidates

    def _create_output_dir(self) -> str:
        """根据时间戳创建输出目录"""
        base_name = datetime.now().strftime('%y-%m-%d-%H-%M')
        candidate = os.path.join(self.base_output_dir, base_name)
        suffix = 1
        while os.path.exists(candidate):
            candidate = os.path.join(self.base_output_dir, f"{base_name}_{suffix}")
            suffix += 1
        os.makedirs(candidate, exist_ok=True)
        if suffix > 1:
            logger.warning(f"检测到同一分钟内多次运行，输出目录调整为 {os.path.basename(candidate)}")
        return candidate

    def _bootstrap_from_previous_run(self):
        """从最近一次运行复制必要的产物（如数据库）"""
        if not self.update_mode:
            return
        if not self.previous_run_dir:
            return
        prev_db = os.path.join(self.previous_run_dir, 'vcb_data.db')
        if os.path.exists(prev_db) and not os.path.exists(self.db_path):
            shutil.copy2(prev_db, self.db_path)
            logger.info(f"已从历史运行复制数据库: {prev_db}")

    def _configure_file_logger(self):
        """为当前运行配置独立的日志文件"""
        for handler in list(logger.handlers):
            if isinstance(handler, logging.FileHandler):
                logger.removeHandler(handler)
                try:
                    handler.close()
                except Exception:
                    pass
        file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    def _find_latest_artifact(self, filename: str) -> Optional[str]:
        """在历史运行目录中查找指定文件的最新版本"""
        for dir_path in reversed(self.historic_run_dirs):
            candidate = os.path.join(dir_path, filename)
            if os.path.exists(candidate):
                return candidate
        return None

    def _html_snippet(self, block) -> str:
        """提取有限长度的 HTML 片段"""
        if not block:
            return ""
        snippet = str(block)
        return snippet[:2000]

    def _normalize_date_text(self, date_text: str) -> str:
        """标准化日期格式为 YY-MM-DD"""
        if not date_text:
            return ""
        date_text = date_text.strip()
        match = re.match(r'^\s*(\d{2})-(\d{1,2})-(\d{1,2})\s*$', date_text)
        if match:
            yy, mm, dd = match.groups()
            return f"{yy}-{int(mm):02d}-{int(dd):02d}"
        # 兼容 YYYY-MM-DD
        match_full = re.match(r'^\s*(\d{4})-(\d{1,2})-(\d{1,2})\s*$', date_text)
        if match_full:
            yyyy, mm, dd = match_full.groups()
            return f"{yyyy[2:]}-{int(mm):02d}-{int(dd):02d}"
        return date_text

    def _record_page_metadata(self, url: str, date_text: str, tags_text: str, html_snippet: str):
        """记录列表页解析到的元数据"""
        if not url:
            return
        metadata = self.page_metadata.setdefault(url, {})
        if date_text:
            normalized = self._normalize_date_text(date_text)
            if normalized:
                metadata['date'] = normalized
        if tags_text:
            metadata['tags'] = tags_text
        if html_snippet:
            metadata['listing_html'] = html_snippet

    def _extract_listing_metadata(self, block) -> Tuple[str, str]:
        """从列表块中提取日期和标签"""
        if not block:
            return "", ""
        date_text = ""
        tags = []
        spans = block.find_all('span')
        for span in spans:
            span_str = str(span)
            if 'fa-calendar' in span_str and not date_text:
                extracted = span.get_text(strip=True)
                date_text = self._normalize_date_text(extracted)
            elif 'fa-tags' in span_str and not tags:
                links = span.find_all('a')
                tags = [link.get_text(strip=True) for link in links if link.get_text(strip=True)]
        return date_text, ','.join(tags)

    def _has_excluded_tag(self, tags_text: str) -> bool:
        """判断标签是否命中黑名单"""
        if not tags_text:
            return False
        tag_list = [tag.strip() for tag in tags_text.split(',') if tag.strip()]
        return any(tag in self.excluded_tags for tag in tag_list)

    def _should_skip_url(self, url: str, log_skip: bool = False) -> bool:
        """根据已知元数据判断是否需要跳过帖子"""
        metadata = self.page_metadata.get(url)
        if metadata and self._has_excluded_tag(metadata.get('tags', '')):
            if log_skip:
                logger.info(f"跳过黑名单标签帖子: {url}")
            return True
        return False

    def _resolve_failure_file(self, failure_file: Optional[str]) -> Optional[str]:
        """确定要使用的失败日志路径"""
        if failure_file and os.path.exists(failure_file):
            return failure_file
        if os.path.exists(self.failure_log_file):
            return self.failure_log_file
        if self.previous_failure_file and os.path.exists(self.previous_failure_file):
            return self.previous_failure_file
        return None

    def _log_metadata_failure(self, field: str, post_url: str, html_snippet: str):
        """当元数据缺失时输出调试日志"""
        snippet = html_snippet or "（空 HTML）"
        logger.error(f"无法提取{field}: {post_url}\nHTML片段:\n{snippet}")
    
    def _init_database(self):
        """初始化 SQLite 数据库"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS posts (
                    url TEXT PRIMARY KEY,
                    title TEXT,
                    date TEXT,
                    tags TEXT,
                    downloads TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
        logger.info("数据库初始化完成")
    
    def _is_url_exists(self, url: str) -> bool:
        """
        检查URL是否已存在于数据库中
        :param url: 帖子URL
        :return: True 如果存在，False 如果不存在
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM posts WHERE url = ?', (url,))
            count = cursor.fetchone()[0]
            return count > 0
    
    def _save_checkpoint(self, page_num: int, collected_urls: List[str]):
        """
        保存断点信息
        :param page_num: 当前页码
        :param collected_urls: 已收集的URL列表
        """
        with self.checkpoint_lock:
            checkpoint = {
                'page_num': page_num,
                'collected_urls': collected_urls,
                'timestamp': datetime.now().isoformat()
            }
            try:
                with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                    json.dump(checkpoint, f, ensure_ascii=False, indent=2)
                logger.debug(f"保存断点: 页码 {page_num}, URL数量 {len(collected_urls)}")
            except Exception as e:
                logger.error(f"保存断点失败: {e}")
    
    def _load_checkpoint(self) -> Optional[Dict]:
        """
        加载断点信息
        :return: 断点信息字典，如果不存在返回None
        """
        try:
            checkpoint_path = self.checkpoint_file
            if not os.path.exists(checkpoint_path):
                historical = self._find_latest_artifact('scraper_checkpoint.json')
                if historical and os.path.exists(historical):
                    checkpoint_path = historical
                    try:
                        shutil.copy2(historical, self.checkpoint_file)
                        logger.info(f"已复制历史断点文件到当前目录: {historical}")
                    except Exception as copy_err:
                        logger.error(f"复制历史断点失败: {copy_err}")
            if os.path.exists(checkpoint_path):
                with open(checkpoint_path, 'r', encoding='utf-8') as f:
                    checkpoint = json.load(f)
                logger.info(f"发现断点: 页码 {checkpoint['page_num']}, "
                           f"已收集 {len(checkpoint['collected_urls'])} 个URL")
                return checkpoint
        except Exception as e:
            logger.error(f"加载断点失败: {e}")
        return None
    
    def _clear_checkpoint(self):
        """清除断点文件"""
        try:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
                logger.info("已清除断点文件")
        except Exception as e:
            logger.error(f"清除断点文件失败: {e}")
    
    def _smart_sleep(self, retry_count: int = 0):
        """
        智能延迟，根据重试次数动态调整
        :param retry_count: 当前重试次数
        """
        # 线性退避，保证重试等待时间逐步增长
        multiplier = 1 if retry_count == 0 else (retry_count + 1)
        sleep_time = self.base_sleep_time * multiplier
        sleep_time = min(sleep_time, self.max_retry_sleep)
        
        # 添加随机抖动，避免整齐的请求模式
        sleep_time += random.uniform(0, 1)
        
        logger.debug(f"延迟 {sleep_time:.2f} 秒")
        time.sleep(sleep_time)
    
    def _request_with_retry(self, url: str, max_retries: int = None, context: str = "") -> Optional[requests.Response]:
        """
        带重试机制的请求
        :param url: 请求URL
        :param max_retries: 最大重试次数
        :return: Response对象或None
        """
        if max_retries is None:
            max_retries = self.max_retries
        
        context_info = f" [{context}]" if context else ""
        
        for attempt in range(max_retries + 1):
            try:
                # 请求前延迟
                if attempt > 0:
                    logger.warning(f"重试 {attempt}/{max_retries}: {url}{context_info}")
                    self._smart_sleep(attempt)
                elif self.request_count > 0 and self.request_count % 10 == 0:
                    # 每10个请求主动延迟
                    self._smart_sleep(0)
                
                # 发送请求
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                # 增加请求计数
                with self.request_lock:
                    self.request_count += 1
                
                return response
                
            except requests.Timeout:
                logger.warning(f"请求超时 (尝试 {attempt + 1}/{max_retries + 1}): {url}{context_info}")
            except requests.HTTPError as e:
                if e.response.status_code == 429:  # Too Many Requests
                    logger.warning(f"触发限流 (尝试 {attempt + 1}/{max_retries + 1}): {url}{context_info}")
                    self._smart_sleep(attempt + 2)  # 限流时延迟更久
                else:
                    logger.error(f"HTTP错误 {e.response.status_code}: {url}{context_info}")
            except Exception as e:
                logger.error(f"请求异常 (尝试 {attempt + 1}/{max_retries + 1}): {url}{context_info} - {e}")
        
        logger.error(f"请求失败，已重试 {max_retries} 次: {url}{context_info}")
        return None
    
    def collect_new_posts(self) -> List[str]:
        """
        收集需要爬取的新帖子（增量更新模式）
        从第1页开始串行遍历，直到遇到已存在的帖子
        :return: 新帖子URL列表
        """
        logger.info("开始收集新帖子（增量更新模式）...")
        new_posts = []
        page_num = 1
        
        while page_num <= self.max_pages:
            logger.info(f"检查第 {page_num} 页是否有新帖子...")
            post_links = self.get_page_post_links(page_num)
            
            if not post_links:
                logger.warning(f"第 {page_num} 页没有找到帖子，停止扫描")
                break
            
            # 检查每个帖子是否存在
            page_has_new = False
            for link in post_links:
                full_url = urljoin(self.base_url, link)
                if self._should_skip_url(full_url):
                    continue
                if not self._is_url_exists(full_url):
                    new_posts.append(full_url)
                    page_has_new = True
                    logger.info(f"发现新帖子: {full_url}")
            
            # 如果当前页没有任何新帖子，说明后续页面都是旧的，可以停止
            if not page_has_new:
                logger.info(f"第 {page_num} 页所有帖子都已存在，停止扫描")
                break
            
            page_num += 1
            self._smart_sleep(0)  # 使用智能延迟
        
        logger.info(f"共发现 {len(new_posts)} 个新帖子")
        return new_posts
    
    def get_page_post_links(self, page_num: int) -> List[str]:
        """
        获取指定页面的所有帖子链接
        :param page_num: 页码
        :return: 帖子链接列表
        """
        url = f"{self.base_url}/page/{page_num}"
        logger.info(f"正在获取第 {page_num} 页的帖子列表: {url}")
        
        response = self._request_with_retry(url)
        if not response:
            logger.error(f"获取第 {page_num} 页失败")
            return []
        
        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            filtered_links: List[str] = []
            seen_urls = set()
            skipped_by_tag = 0
            skipped_duplicates = 0
            
            def register_link(href: str, block):
                nonlocal skipped_by_tag, skipped_duplicates
                if not href or '/archives/' not in href:
                    return
                if 'comment' in href:
                    return
                post_id = re.search(r'/archives/(\d+)', href)
                if not post_id:
                    return
                full_url = urljoin(self.base_url, href)
                if full_url in seen_urls:
                    skipped_duplicates += 1
                    return
                
                listing_html = self._html_snippet(block)
                date_meta, tags_meta = self._extract_listing_metadata(block)
                self._record_page_metadata(full_url, date_meta, tags_meta, listing_html)
                
                if self._should_skip_url(full_url, log_skip=True):
                    seen_urls.add(full_url)
                    skipped_by_tag += 1
                    return
                
                filtered_links.append(href)
                seen_urls.add(full_url)
            
            # 方法1: 通过文章容器查找
            articles = soup.find_all('div', class_='article')
            for article in articles:
                title_link = article.find('h1') or article.find('h4')
                if not title_link:
                    continue
                link = title_link.find('a')
                if link and link.get('href'):
                    register_link(link['href'], article)
            
            # 方法2: 备用模式，仅在正文解析失败时使用
            if not filtered_links:
                article_container = soup.select('#article-list')
                search_scope = article_container[0] if article_container else soup
                all_links = search_scope.find_all('a', href=re.compile(r'/archives/\d+'))
                for link in all_links:
                    href = link.get('href')
                    if not href:
                        continue
                    block = link.find_parent('div', class_='article') or link.find_parent('section')
                    register_link(href, block)
            
            extra_info = []
            if skipped_by_tag:
                extra_info.append(f"跳过黑名单 {skipped_by_tag} 个")
            if skipped_duplicates:
                extra_info.append(f"重复 {skipped_duplicates} 个")
            suffix = f"（{', '.join(extra_info)}）" if extra_info else ""
            logger.info(f"第 {page_num} 页找到 {len(filtered_links)} 个帖子{suffix}")
            return filtered_links
            
        except Exception as e:
            logger.error(f"解析第 {page_num} 页失败: {e}")
            return []
    
    def extract_post_info(self, post_url: str) -> Optional[Dict]:
        """
        提取单个帖子的信息
        :param post_url: 帖子URL
        :return: 帖子信息字典
        """
        if not post_url.startswith('http'):
            post_url = urljoin(self.base_url, post_url)
        
        logger.info(f"正在处理帖子: {post_url}")
        metadata = self.page_metadata.get(post_url, {})
        listing_html = metadata.get('listing_html', '')
        
        response = self._request_with_retry(post_url)
        if not response:
            logger.error(f"获取帖子失败: {post_url}")
            return None
        
        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 提取标题
            title = self._extract_title(soup)
            
            # 提取日期
            date = self._extract_date(
                soup,
                post_url,
                prefetched_date=metadata.get('date', ''),
                listing_html=listing_html
            )
            
            # 提取标签
            tags = self._extract_tags(
                soup,
                post_url,
                prefetched_tags=metadata.get('tags', ''),
                listing_html=listing_html
            )
            
            # 提取下载信息
            downloads = self._extract_downloads(soup, post_url)
            
            if post_url not in self.page_metadata:
                self.page_metadata[post_url] = {}
            if date and not self.page_metadata[post_url].get('date'):
                self.page_metadata[post_url]['date'] = date
            if tags and not self.page_metadata[post_url].get('tags'):
                self.page_metadata[post_url]['tags'] = tags
            
            if not title:
                logger.warning(f"无法提取标题: {post_url}")
                return None
            
            post_info = {
                'url': post_url,
                'title': title,
                'date': date,
                'tags': tags,
                'downloads': downloads
            }
            
            logger.info(f"成功提取帖子信息: {title}")
            return post_info
            
        except Exception as e:
            logger.error(f"处理帖子失败 {post_url}: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """提取标题"""
        # 方法1: 通过 title-article 类
        title_div = soup.find('div', class_='title-article')
        if title_div:
            h1 = title_div.find('h1') or title_div.find('h4')
            if h1:
                a = h1.find('a')
                if a:
                    return a.get_text(strip=True)
        
        # 方法2: 直接查找 h1 标签
        h1 = soup.find('h1')
        if h1:
            return h1.get_text(strip=True)
        
        # 方法3: 从页面 title 标签提取
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            # 移除网站名称
            title_text = re.sub(r'\s*[–-]\s*VCB-Studio.*$', '', title_text)
            return title_text
        
        return ""
    
    def _extract_date(
        self,
        soup: BeautifulSoup,
        post_url: str,
        prefetched_date: str = "",
        listing_html: str = ""
    ) -> str:
        """提取日期，优先使用列表页解析结果"""
        normalized_prefetch = self._normalize_date_text(prefetched_date)
        if normalized_prefetch:
            return normalized_prefetch
        
        # 方法1: 通过 fa-calendar 图标
        calendar_spans = soup.find_all('span', class_='label')
        for span in calendar_spans:
            if 'fa-calendar' in str(span):
                date_text = span.get_text(strip=True)
                normalized = self._normalize_date_text(date_text)
                if normalized:
                    return normalized
        
        # 方法2: 查找日期格式
        date_pattern = re.compile(r'(\d{2})-(\d{1,2})-(\d{1,2})')
        text_content = soup.get_text()
        match = date_pattern.search(text_content)
        if match:
            yy, mm, dd = match.groups()
            return f"{yy}-{int(mm):02d}-{int(dd):02d}"
        
        snippet = listing_html or self._html_snippet(soup)
        self._log_metadata_failure("发布日期", post_url, snippet)
        return ""
    
    def _extract_tags(
        self,
        soup: BeautifulSoup,
        post_url: str,
        prefetched_tags: str = "",
        listing_html: str = ""
    ) -> str:
        """提取标签，优先使用列表页解析结果"""
        if prefetched_tags:
            return prefetched_tags
        
        tags = []
        
        # 方法1: 通过 fa-tags 图标
        tag_spans = soup.find_all('span', class_='label')
        for span in tag_spans:
            if 'fa-tags' in str(span):
                links = span.find_all('a')
                for link in links:
                    tag_text = link.get_text(strip=True)
                    if tag_text:
                        tags.append(tag_text)
        
        # 方法2: 查找分类链接
        if not tags:
            category_links = soup.find_all('a', rel='category tag')
            tags = [link.get_text(strip=True) for link in category_links if link.get_text(strip=True)]
        
        tag_text = ','.join(tags)
        if not tag_text:
            snippet = listing_html or self._html_snippet(soup)
            self._log_metadata_failure("标签", post_url, snippet)
        return tag_text
    
    def _extract_downloads(self, soup: BeautifulSoup, post_url: str) -> List[Dict]:
        """提取下载信息"""
        downloads = []
        
        # 查找所有下载框
        download_boxes = soup.find_all('div', class_='dw-box-download')
        
        for box in download_boxes:
            # 提取格式信息（第一行文本）
            format_text = ""
            for content in box.contents:
                if isinstance(content, str):
                    text = content.strip()
                    if text and text != '/' and 'icon-download' not in text:
                        format_text = text
                        break
            
            if not format_text:
                # 尝试从 br 标签后的文本获取
                br = box.find('br')
                if br and br.next_sibling:
                    format_text = str(br.next_sibling).strip()
            
            # 提取所有下载链接
            links = []
            for a in box.find_all('a', href=True):
                if a.find_parent('del'):
                    continue  # 标记为删除的链接视为无效
                href = a['href']
                if href and href.startswith('http'):
                    links.append(href)
            
            if not links:
                logger.info(f"跳过已失效的下载格式 [{format_text or '未命名'}] - {post_url}")
                continue
            
            # 获取磁链
            magnet = self._get_magnet_from_links(links, post_url, format_text)
            
            download_info = {
                'format': format_text,
                'links': links,
                'magnet': magnet
            }
            downloads.append(download_info)
        
        return downloads
    
    def _get_magnet_from_links(self, links: List[str], post_url: str = "", format_label: str = "") -> str:
        """
        从链接列表中获取磁链
        :param links: 下载链接列表
        :param post_url: 所属帖子
        :param format_label: 下载格式描述
        :return: 磁链字符串
        """
        # 按优先级排序链接
        sorted_links = []
        for domain in self.magnet_domains:
            for link in links:
                if domain in link:
                    sorted_links.append(link)
        
        # 添加剩余链接
        for link in links:
            if link not in sorted_links:
                sorted_links.append(link)
        
        # 尝试从每个链接获取磁链
        attempted_links = []
        for link in sorted_links:
            attempted_links.append(link)
            # 修正 dmhy.org 链接
            if 'dmhy.org' in link and 'share.dmhy.org' not in link:
                link = link.replace('dmhy.org', 'share.dmhy.org')
            
            magnet = self._fetch_magnet_from_url(link, post_url, format_label)
            if magnet:
                self._record_magnet_success(post_url, link, format_label, magnet)
                return magnet
        
        self._record_magnet_failure(
            post_url,
            attempted_links,
            format_label,
            "所有候选链接均未返回磁链",
            final=True
        )
        
        return ""
    
    def _fetch_magnet_from_url(self, url: str, post_url: str = "", format_label: str = "") -> str:
        """
        从指定 URL 获取磁链
        :param url: 下载页面 URL
        :param post_url: 所属帖子 URL
        :param format_label: 下载格式描述
        :return: 磁链字符串
        """
        logger.debug(f"尝试从 {url} 获取磁链")
        
        context = f"{format_label or '未命名'} | {post_url}"
        response = self._request_with_retry(url, max_retries=self.max_retries, context=context)
        if not response:
            self._record_magnet_failure(post_url, url, format_label, "请求失败", final=False)
            logger.warning(f"无法访问 {url}")
            return ""
        
        try:
            # 查找磁链
            content = response.text
            
            # 方法1: 查找完整的 magnet 链接
            magnet_pattern = re.compile(r'magnet:\?xt=urn:btih:[a-zA-Z0-9&=:%._\+~#?/\-]+')
            matches = magnet_pattern.findall(content)
            if matches:
                # 返回最长的磁链（通常包含更多 tracker）
                return max(matches, key=len)
            
            # 方法2: 从 HTML 属性中查找
            soup = BeautifulSoup(content, 'html.parser')
            magnet_links = soup.find_all('a', href=re.compile(r'^magnet:'))
            if magnet_links:
                return magnet_links[0]['href']
            
            # 方法3: 查找包含 magnet 的输入框或文本
            inputs = soup.find_all('input', {'value': re.compile(r'^magnet:')})
            if inputs:
                return inputs[0]['value']
            
            logger.warning(f"未在 {url} 找到磁链")
            self._record_magnet_failure(post_url, url, format_label, "页面中无磁链", final=False)
            return ""
            
        except Exception as e:
            logger.error(f"解析磁链失败 {url}: {e}")
            self._record_magnet_failure(post_url, url, format_label, f"解析异常: {e}", final=False)
            return ""

    def _record_magnet_success(self, post_url: str, source_url: str, format_label: str, magnet: str):
        """记录磁链成功信息"""
        entry = {
            'post_url': post_url,
            'format': format_label,
            'source_url': source_url,
            'magnet_preview': magnet[:80],
            'timestamp': datetime.now().isoformat()
        }
        self.magnet_success_records.append(entry)
        logger.info(f"磁链获取成功 [{format_label or '未命名'}] 来自 {source_url}")

    def _record_magnet_failure(self, post_url: str, source, format_label: str, reason: str, final: bool):
        """记录磁链失败信息"""
        entry = {
            'post_url': post_url,
            'format': format_label,
            'source': source,
            'reason': reason,
            'final': final,
            'timestamp': datetime.now().isoformat()
        }
        trigger_write = False
        with self.failure_lock:
            self.magnet_failure_records.append(entry)
            if final and post_url:
                self.posts_with_failed_magnets.add(post_url)
            if final:
                trigger_write = True
        if trigger_write:
            self._write_failure_log()
    
    def save_to_database(self, post_info: Dict):
        """保存到数据库"""
        with self.db_lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT OR REPLACE INTO posts (url, title, date, tags, downloads)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        post_info['url'],
                        post_info['title'],
                        post_info['date'],
                        post_info['tags'],
                        json.dumps(post_info['downloads'], ensure_ascii=False)
                    ))
                    conn.commit()
                logger.debug(f"已保存到数据库: {post_info['title']}")
            except Exception as e:
                logger.error(f"保存到数据库失败: {e}")
    
    def export_to_excel(self, output_file: Optional[str] = None):
        """导出到 Excel 文件（按日期排序）"""
        if output_file is None:
            output_file = self.excel_path
        with self.excel_lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    # 读取数据
                    df = pd.read_sql_query(
                        "SELECT * FROM posts ORDER BY date ASC",
                        conn
                    )
                    
                    # 准备基础列
                    excel_data = []
                    
                    for _, row in df.iterrows():
                        # 处理标题：将 / 替换为 &
                        title = row['title'].replace('/', '&') if row['title'] else ""
                        
                        # 处理日期：确保格式为 YY-MM-DD
                        date = row['date'] if row['date'] else ""
                        # 如果日期格式不对，尝试转换
                        if date and not re.match(r'^\d{2}-\d{2}-\d{2}$', date):
                            # 尝试其他格式的转换
                            try:
                                # 假设可能是 YYYY-MM-DD 格式
                                if re.match(r'^\d{4}-\d{2}-\d{2}$', date):
                                    parts = date.split('-')
                                    date = f"{parts[0][2:]}-{parts[1]}-{parts[2]}"
                            except:
                                pass
                        
                        # 解析下载信息
                        downloads = json.loads(row['downloads']) if row['downloads'] else []
                        
                        # 构建行数据
                        row_data = {
                            '发布日期': date,
                            '页面网址': row['url'],
                            '标题信息': title,
                            '标签': row['tags']
                        }
                        
                        # 添加下载格式和磁链（动态列）
                        for i, dl in enumerate(downloads, 1):
                            format_text = dl.get('format', 'N/A').strip()
                            magnet = dl.get('magnet', '未获取')
                            
                            if i == 1:
                                row_data['下载格式'] = format_text
                                row_data['下载格式对应的磁链'] = magnet
                            else:
                                row_data[f'下载格式{i}'] = format_text
                                row_data[f'下载格式{i}对应的磁链'] = magnet
                        
                        # 如果没有下载信息，也要添加空列
                        if not downloads:
                            row_data['下载格式'] = ''
                            row_data['下载格式对应的磁链'] = ''
                        
                        excel_data.append(row_data)
                    
                    # 创建 DataFrame
                    result_df = pd.DataFrame(excel_data)
                    
                    # 确保列的顺序
                    base_columns = ['发布日期', '页面网址', '标题信息', '标签', '下载格式', '下载格式对应的磁链']
                    
                    # 找出所有额外的下载格式列
                    extra_columns = []
                    for col in result_df.columns:
                        if col not in base_columns:
                            extra_columns.append(col)
                    
                    # 对额外列进行排序（按数字顺序）
                    extra_columns.sort(key=lambda x: (
                        int(re.search(r'\d+', x).group()) if re.search(r'\d+', x) else 0,
                        '磁链' in x  # 格式列在前，磁链列在后
                    ))
                    
                    # 最终列顺序
                    final_columns = base_columns + extra_columns
                    result_df = result_df.reindex(columns=final_columns, fill_value='')
                    
                    # 导出到 Excel
                    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                        result_df.to_excel(writer, index=False, sheet_name='VCB-S 帖子')
                        
                        # 调整列宽
                        worksheet = writer.sheets['VCB-S 帖子']
                        worksheet.column_dimensions['A'].width = 50  # 页面网址
                        worksheet.column_dimensions['B'].width = 70  # 标题信息
                        worksheet.column_dimensions['C'].width = 12  # 发布日期
                        worksheet.column_dimensions['D'].width = 30  # 标签
                        
                        # 动态调整下载格式和磁链列的宽度
                        for i, col in enumerate(final_columns[4:], start=5):
                            col_letter = chr(64 + i)  # E, F, G, H...
                            if '磁链' in col:
                                worksheet.column_dimensions[col_letter].width = 100
                            else:
                                worksheet.column_dimensions[col_letter].width = 25
                    
                    logger.info(f"已导出到 Excel: {output_file} (共 {len(result_df)} 条记录)")
                    
            except Exception as e:
                logger.error(f"导出到 Excel 失败: {e}")
                logger.error(traceback.format_exc())
    
    def process_posts_in_batches(self, post_urls: List[str]):
        """
        使用滑动窗口批量处理帖子
        :param post_urls: 帖子URL列表
        """
        total = len(post_urls)
        success_count = 0
        
        # 按窗口大小分批处理
        for batch_start in range(0, total, self.window_size):
            batch_end = min(batch_start + self.window_size, total)
            batch = post_urls[batch_start:batch_end]
            
            logger.info(f"处理批次 {batch_start//self.window_size + 1}/{(total + self.window_size - 1)//self.window_size}: "
                       f"帖子 {batch_start + 1}-{batch_end}/{total}")
            
            # 并行处理当前批次
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = {executor.submit(self._process_single_post, url): url for url in batch}
                
                for future in as_completed(futures):
                    url = futures[future]
                    try:
                        if future.result():
                            success_count += 1
                            logger.info(f"总进度: {success_count}/{total}")
                    except Exception as e:
                        logger.error(f"处理帖子异常 {url}: {e}")
                        added = False
                        with self.failure_lock:
                            if url not in self.failed_posts:
                                self.failed_posts.add(url)
                                added = True
                        if added:
                            self._write_failure_log()
            
            # 批次完成后导出一次
            self.export_to_excel()
            logger.info(f"批次处理完成，已导出 Excel")
        
        return success_count
    
    def _process_single_post(self, post_url: str) -> bool:
        """
        处理单个帖子（包括保存）
        :param post_url: 帖子URL
        :return: 是否成功
        """
        if self._should_skip_url(post_url, log_skip=True):
            return True
        
        post_info = self.extract_post_info(post_url)
        if post_info:
            if self._has_excluded_tag(post_info.get('tags', '')):
                logger.info(f"帖子标签命中黑名单，跳过保存: {post_url}")
                return True
            removed = False
            with self.failure_lock:
                if post_url in self.failed_posts:
                    self.failed_posts.discard(post_url)
                    removed = True
            if removed:
                self._write_failure_log()
            self.save_to_database(post_info)
            return True
        
        added = False
        with self.failure_lock:
            if post_url not in self.failed_posts:
                self.failed_posts.add(post_url)
                added = True
        if added:
            self._write_failure_log()
        return False

    def _write_failure_log(self):
        """将失败信息写入文件，便于后续重试"""
        with self.failure_lock:
            post_failures = sorted(self.failed_posts)
            magnet_failures = [
                entry for entry in self.magnet_failure_records if entry.get('final')
            ]
        failure_summary = {
            'generated_at': datetime.now().isoformat(),
            'post_failures': post_failures,
            'magnet_failures': magnet_failures
        }
        
        if not failure_summary['post_failures'] and not failure_summary['magnet_failures']:
            with self.failure_log_lock:
                if os.path.exists(self.failure_log_file):
                    os.remove(self.failure_log_file)
                    logger.info(f"没有失败记录，已删除 {self.failure_log_file}")
                self.previous_failure_file = None
            return
        
        try:
            with self.failure_log_lock:
                with open(self.failure_log_file, 'w', encoding='utf-8') as f:
                    json.dump(failure_summary, f, ensure_ascii=False, indent=2)
                self.previous_failure_file = self.failure_log_file
            logger.warning(f"存在失败记录，详情已写入 {self.failure_log_file}")
        except Exception as e:
            logger.error(f"写入失败日志 {self.failure_log_file} 失败: {e}")

    def _report_magnet_summary(self):
        """输出磁链成功/失败统计"""
        success_count = len(self.magnet_success_records)
        final_failures = [entry for entry in self.magnet_failure_records if entry.get('final')]
        failure_count = len(final_failures)
        with self.failure_lock:
            magnet_post_count = len(self.posts_with_failed_magnets)
            failed_post_count = len(self.failed_posts)
        
        logger.info(f"磁链获取成功 {success_count} 条")
        if failure_count:
            logger.warning(
                f"磁链获取失败 {failure_count} 条，涉及 {magnet_post_count} 个帖子，需要后续重试"
            )
        else:
            logger.info("所有磁链均成功获取")
        
        if failed_post_count:
            logger.warning(f"帖子解析失败 {failed_post_count} 个，稍后可通过 --retry-failed 重试")
        
        self._write_failure_log()

    def retry_failed_posts(self, failure_file: Optional[str] = None):
        """
        读取失败日志，重新处理失败的帖子
        :param failure_file: 失败日志路径
        """
        failure_file = self._resolve_failure_file(failure_file)
        if not failure_file:
            logger.error("未找到失败日志，请确认是否已生成失败记录")
            return
        logger.info(f"读取失败日志: {failure_file}")
        
        try:
            with open(failure_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f"读取失败日志失败: {e}")
            return
        
        post_failures = set(data.get('post_failures', []))
        magnet_failures = {
            entry.get('post_url') for entry in data.get('magnet_failures', [])
            if entry.get('post_url')
        }
        target_posts = sorted(post_failures.union(magnet_failures))
        
        if not target_posts:
            logger.info("失败日志中没有需要重试的帖子")
            return
        
        logger.info(f"准备重试 {len(target_posts)} 个帖子")
        
        # 清空上一轮的统计，避免混淆
        with self.failure_lock:
            self.failed_posts.clear()
            self.posts_with_failed_magnets.clear()
        self.magnet_success_records.clear()
        self.magnet_failure_records.clear()
        
        success_count = self.process_posts_in_batches(target_posts)
        logger.info(f"失败重试完成，成功 {success_count}/{len(target_posts)} 个帖子")
        
        self._report_magnet_summary()
    
    def run(self):
        """运行爬虫"""
        start_time = time.time()
        
        if self.update_mode:
            # 增量更新模式：只爬取新帖子
            logger.info("="*60)
            logger.info("运行模式：增量更新")
            logger.info(f"并行度: {self.workers}, 窗口大小: {self.window_size}")
            logger.info("="*60)
            
            # 串行收集新帖子
            new_posts = self.collect_new_posts()
            
            if not new_posts:
                logger.info("没有发现新帖子，无需更新")
                return
            
            # 使用滑动窗口批量处理
            success_count = self.process_posts_in_batches(new_posts)
            
        else:
            # 完整爬取模式
            logger.info("="*60)
            logger.info(f"运行模式：完整爬取（前 {self.max_pages} 页）")
            logger.info(f"并行度: {self.workers}, 窗口大小: {self.window_size}")
            logger.info("="*60)
            
            # 检查是否有断点
            checkpoint = self._load_checkpoint()
            start_page = 1
            all_post_links = []
            
            if checkpoint:
                # 从断点恢复
                start_page = checkpoint['page_num']
                all_post_links = checkpoint['collected_urls']
                logger.info(f"从第 {start_page} 页恢复爬取，已收集 {len(all_post_links)} 个URL")
                
                # 询问用户是否继续
                logger.info("发现断点，将继续上次的爬取进度")
            
            # 收集所有帖子链接（从断点页开始）
            for page_num in range(start_page, self.max_pages + 1):
                links = self.get_page_post_links(page_num)
                # 转换为完整URL
                full_urls = [urljoin(self.base_url, link) for link in links]
                for full_url in full_urls:
                    if self._should_skip_url(full_url):
                        continue
                    all_post_links.append(full_url)
                
                # 保存断点（每10页保存一次）
                if page_num % 10 == 0 or page_num == self.max_pages:
                    self._save_checkpoint(page_num + 1, all_post_links)
                
                # 主动延迟
                self._smart_sleep(0)
            
            logger.info(f"共找到 {len(all_post_links)} 个帖子")
            
            # 过滤已存在的帖子（宁可重复也不缺失）
            # 数据库中已有的会被跳过，断点续抓时可能会有重复但不会缺失
            new_posts = []
            for url in all_post_links:
                if self._should_skip_url(url):
                    continue
                if not self._is_url_exists(url):
                    new_posts.append(url)
            
            existing_count = len(all_post_links) - len(new_posts)
            
            logger.info(f"其中 {existing_count} 个已存在，{len(new_posts)} 个需要爬取")
            
            if not new_posts:
                logger.info("所有帖子都已存在，无需爬取")
                self._clear_checkpoint()
                return
            
            # 使用滑动窗口批量处理
            success_count = self.process_posts_in_batches(new_posts)
            
            # 完成后清除断点
            self._clear_checkpoint()
        
        # 最终统计
        elapsed_time = time.time() - start_time
        logger.info("="*60)
        logger.info(f"爬取完成！")
        logger.info(f"成功处理: {success_count} 个帖子")
        logger.info(f"总耗时: {elapsed_time:.2f} 秒")
        if success_count > 0:
            logger.info(f"平均速度: {success_count / elapsed_time * 60:.2f} 帖子/分钟")
        
        self._report_magnet_summary()
        logger.info("="*60)


def main():
    parser = argparse.ArgumentParser(
        description='VCB-S 网站爬虫 - 支持完整爬取和增量更新',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 增量更新（推荐）：自动从第1页开始，只爬取新帖子
  python vcb_scraper.py --update
  
  # 完整爬取：爬取前10页
  python vcb_scraper.py --max-pages 10
  
  # 自定义并行度和窗口大小
  python vcb_scraper.py --update --workers 8 --window-size 100
        """
    )
    parser.add_argument('--max-pages', type=int, default=163, 
                       help='最大爬取页数（默认163，仅在非更新模式下有效）')
    parser.add_argument('--workers', type=int, default=4, 
                       help='并行线程数（默认4）')
    parser.add_argument('--window-size', type=int, default=50,
                       help='滑动窗口大小，每次处理的帖子数量（默认50）')
    parser.add_argument('--update', action='store_true',
                       help='增量更新模式：自动从第1页开始，只爬取新帖子')
    parser.add_argument('--retry-failed', action='store_true',
                       help='读取失败日志，仅重试失败的帖子')
    args = parser.parse_args()
    
    scraper = VCBScraper(
        max_pages=args.max_pages,
        workers=args.workers,
        window_size=args.window_size,
        update_mode=args.update
    )
    
    if args.retry_failed:
        scraper.retry_failed_posts()
        return
    scraper.run()


if __name__ == '__main__':
    main()
