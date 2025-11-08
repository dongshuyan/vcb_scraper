#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
读取 vcb_data.db 数据库的脚本
"""

import sqlite3
import json
import argparse
import os
from typing import List, Dict, Optional


def find_latest_db() -> Optional[str]:
    """查找最新的数据库文件"""
    base_dir = os.getcwd()
    db_files = []
    
    # 查找所有日期格式的文件夹
    for entry in os.listdir(base_dir):
        if os.path.isdir(os.path.join(base_dir, entry)) and len(entry) == 14 and entry.count('-') == 4:
            db_path = os.path.join(base_dir, entry, 'vcb_data.db')
            if os.path.exists(db_path):
                db_files.append((entry, db_path))
    
    # 按文件夹名排序（日期格式，字符串排序即可）
    if db_files:
        db_files.sort(reverse=True)
        return db_files[0][1]
    
    # 如果没找到，尝试根目录
    root_db = os.path.join(base_dir, 'vcb_data.db')
    if os.path.exists(root_db):
        return root_db
    
    return None


def query_all_posts(db_path: str, limit: Optional[int] = None) -> List[Dict]:
    """查询所有帖子"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    query = "SELECT * FROM posts ORDER BY date ASC"
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    posts = []
    for row in rows:
        post = dict(row)
        # 解析 downloads JSON
        if post['downloads']:
            try:
                post['downloads'] = json.loads(post['downloads'])
            except:
                post['downloads'] = []
        else:
            post['downloads'] = []
        posts.append(post)
    
    conn.close()
    return posts


def query_by_tag(db_path: str, tag: str) -> List[Dict]:
    """按标签查询帖子"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM posts WHERE tags LIKE ? ORDER BY date ASC", (f'%{tag}%',))
    rows = cursor.fetchall()
    
    posts = []
    for row in rows:
        post = dict(row)
        if post['downloads']:
            try:
                post['downloads'] = json.loads(post['downloads'])
            except:
                post['downloads'] = []
        else:
            post['downloads'] = []
        posts.append(post)
    
    conn.close()
    return posts


def query_by_title(db_path: str, keyword: str) -> List[Dict]:
    """按标题关键词查询"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM posts WHERE title LIKE ? ORDER BY date ASC", (f'%{keyword}%',))
    rows = cursor.fetchall()
    
    posts = []
    for row in rows:
        post = dict(row)
        if post['downloads']:
            try:
                post['downloads'] = json.loads(post['downloads'])
            except:
                post['downloads'] = []
        else:
            post['downloads'] = []
        posts.append(post)
    
    conn.close()
    return posts


def get_statistics(db_path: str) -> Dict:
    """获取统计信息"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 总帖子数
    cursor.execute("SELECT COUNT(*) FROM posts")
    total_posts = cursor.fetchone()[0]
    
    # 有磁链的帖子数
    cursor.execute("SELECT COUNT(*) FROM posts WHERE downloads LIKE '%magnet%'")
    posts_with_magnet = cursor.fetchone()[0]
    
    # 日期范围
    cursor.execute("SELECT MIN(date), MAX(date) FROM posts WHERE date != ''")
    date_range = cursor.fetchone()
    
    conn.close()
    
    return {
        'total_posts': total_posts,
        'posts_with_magnet': posts_with_magnet,
        'date_range': date_range
    }


def print_post(post: Dict, show_magnet: bool = True):
    """打印单个帖子信息"""
    print(f"\n{'='*80}")
    print(f"标题: {post['title']}")
    print(f"日期: {post['date']}")
    print(f"标签: {post['tags']}")
    print(f"URL: {post['url']}")
    print(f"创建时间: {post['created_at']}")
    
    if post['downloads']:
        print(f"\n下载格式 ({len(post['downloads'])} 个):")
        for i, dl in enumerate(post['downloads'], 1):
            print(f"  [{i}] {dl.get('format', 'N/A')}")
            if show_magnet and dl.get('magnet'):
                magnet = dl['magnet']
                print(f"      磁链: {magnet[:80]}..." if len(magnet) > 80 else f"      磁链: {magnet}")
            elif show_magnet:
                print(f"      磁链: 未获取")
    else:
        print("\n无下载信息")


def main():
    parser = argparse.ArgumentParser(description='读取 vcb_data.db 数据库')
    parser.add_argument('--db', type=str, help='数据库文件路径（默认自动查找最新的）')
    parser.add_argument('--limit', type=int, help='限制返回数量')
    parser.add_argument('--tag', type=str, help='按标签筛选')
    parser.add_argument('--title', type=str, help='按标题关键词筛选')
    parser.add_argument('--stats', action='store_true', help='显示统计信息')
    parser.add_argument('--no-magnet', action='store_true', help='不显示磁链')
    args = parser.parse_args()
    
    # 查找数据库文件
    if args.db:
        db_path = args.db
    else:
        db_path = find_latest_db()
        if not db_path:
            print("错误: 未找到数据库文件")
            print("请使用 --db 参数指定数据库路径，或确保在项目目录下运行")
            return
    
    if not os.path.exists(db_path):
        print(f"错误: 数据库文件不存在: {db_path}")
        return
    
    print(f"使用数据库: {db_path}\n")
    
    # 显示统计信息
    if args.stats:
        stats = get_statistics(db_path)
        print("="*80)
        print("统计信息")
        print("="*80)
        print(f"总帖子数: {stats['total_posts']}")
        print(f"有磁链的帖子数: {stats['posts_with_magnet']}")
        if stats['date_range'][0]:
            print(f"日期范围: {stats['date_range'][0]} 至 {stats['date_range'][1]}")
        print()
        return
    
    # 查询数据
    if args.tag:
        posts = query_by_tag(db_path, args.tag)
        print(f"找到 {len(posts)} 个包含标签 '{args.tag}' 的帖子")
    elif args.title:
        posts = query_by_title(db_path, args.title)
        print(f"找到 {len(posts)} 个标题包含 '{args.title}' 的帖子")
    else:
        posts = query_all_posts(db_path, args.limit)
        print(f"共 {len(posts)} 个帖子")
    
    # 显示结果
    if posts:
        for post in posts:
            print_post(post, show_magnet=not args.no_magnet)
    else:
        print("未找到匹配的帖子")


if __name__ == '__main__':
    main()

