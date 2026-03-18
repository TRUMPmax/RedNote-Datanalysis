# -*- coding: utf-8 -*-
"""
文本分析器 - 关键词提取、情感分析、词频统计
"""
import re
import json
import os
from typing import List, Dict, Tuple, Any
from collections import Counter


# ──────────────────────────────────────────────────
# 停用词（可外挂文件）
# ──────────────────────────────────────────────────
DEFAULT_STOP_WORDS = {
    "的", "了", "在", "是", "我", "有", "和", "就", "不", "人", "都", "一",
    "一个", "上", "也", "很", "到", "说", "要", "去", "你", "会", "着",
    "没有", "看", "好", "自己", "这", "那", "什么", "但", "如果", "因为",
    "所以", "然后", "这个", "那个", "可以", "还是", "我们", "他们", "她们",
    "其实", "已经", "一直", "感觉", "觉得", "真的", "一些", "这样", "那样",
    "时候", "现在", "以前", "之后", "然后", "因此", "虽然", "不过", "但是",
    "只是", "可是", "而且", "或者", "还有", "应该", "需要", "可能", "应该",
    "比较", "非常", "特别", "真是", "实在", "完全", "绝对", "主要", "基本",
    "哈哈", "哈", "嗯", "啊", "哦", "呢", "吧", "啦", "诶", "嘛",
    "图", "视频", "笔记", "小红书", "分享", "推荐", "评论", "点赞", "收藏"
}

# 情感词典（简版，实际可扩展）
POSITIVE_WORDS = {
    "喜欢", "好看", "好用", "推荐", "优秀", "棒", "赞", "完美", "满意", "舒适",
    "实用", "效果好", "值得", "惊艳", "超好", "爱了", "必买", "回购", "强烈推荐",
    "好评", "开心", "快乐", "满足", "高兴", "幸福", "感动", "贴心", "温馨",
    "清爽", "清新", "精致", "高级", "可爱", "漂亮", "好吃", "好喝", "香",
    "nice", "great", "好用", "稳定", "流畅", "专业", "耐用"
}

NEGATIVE_WORDS = {
    "不好", "差", "难用", "失望", "垃圾", "坑", "骗", "退货", "投诉", "劣质",
    "不推荐", "慎买", "后悔", "浪费", "不值", "便宜货", "假货", "有问题",
    "难吃", "难喝", "臭", "发霉", "变质", "卡顿", "闪退", "崩溃", "掉色",
    "掉毛", "开裂", "褪色", "缩水", "不行", "烂", "破", "发霉",
    "太贵", "性价比低", "不划算", "坑钱"
}


def _try_import_jieba():
    try:
        import jieba
        return jieba
    except ImportError:
        return None


def _simple_tokenize(text: str, stop_words: set) -> List[str]:
    """不依赖 jieba 的简单分词（按字/双字滑窗）"""
    tokens = []
    # 按标点分句
    sentences = re.split(r'[，。！？、；：\n\r]', text)
    for sent in sentences:
        # 提取连续中文字符串
        segments = re.findall(r'[\u4e00-\u9fff]+', sent)
        for seg in segments:
            if len(seg) >= 2 and seg not in stop_words:
                tokens.append(seg)
            # 双字滑窗
            for i in range(len(seg) - 1):
                bigram = seg[i:i+2]
                if bigram not in stop_words:
                    tokens.append(bigram)
    return tokens


class TextAnalyzer:
    """小红书文本分析（标题/描述/评论）"""

    def __init__(self, stop_words_file: str = None):
        self.stop_words = set(DEFAULT_STOP_WORDS)
        if stop_words_file and os.path.exists(stop_words_file):
            try:
                with open(stop_words_file, "r", encoding="utf-8") as f:
                    self.stop_words.update(line.strip() for line in f if line.strip())
            except:
                pass
        self.jieba = _try_import_jieba()
        if self.jieba:
            # 注入领域词
            for w in POSITIVE_WORDS | NEGATIVE_WORDS:
                self.jieba.add_word(w)

    def tokenize(self, text: str) -> List[str]:
        """分词"""
        if not text:
            return []
        text = re.sub(r'[^\u4e00-\u9fffa-zA-Z0-9]', ' ', text)
        if self.jieba:
            words = self.jieba.lcut(text)
        else:
            words = _simple_tokenize(text, set())
        # 过滤停用词和短词
        return [
            w.strip() for w in words
            if len(w.strip()) >= 2
            and w.strip() not in self.stop_words
            and not w.strip().isdigit()
        ]

    def get_word_frequency(self, texts: List[str], top_n: int = 50) -> List[Tuple[str, int]]:
        """词频统计"""
        all_words = []
        for text in texts:
            all_words.extend(self.tokenize(text))
        return Counter(all_words).most_common(top_n)

    def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        """简单情感分析（基于词典匹配）"""
        if not text:
            return {"label": "neutral", "score": 0, "positive_words": [], "negative_words": []}

        tokens = self.tokenize(text)
        token_set = set(tokens)

        pos_found = list(token_set & POSITIVE_WORDS)
        neg_found = list(token_set & NEGATIVE_WORDS)

        pos_score = len(pos_found)
        neg_score = len(neg_found)

        if pos_score > neg_score:
            label = "positive"
            score = round(pos_score / max(pos_score + neg_score, 1), 2)
        elif neg_score > pos_score:
            label = "negative"
            score = round(-neg_score / max(pos_score + neg_score, 1), 2)
        else:
            label = "neutral"
            score = 0

        return {
            "label": label,
            "score": score,
            "positive_words": pos_found,
            "negative_words": neg_found
        }

    def batch_sentiment(self, texts: List[str]) -> Dict[str, Any]:
        """批量情感分析并汇总"""
        results = [self.analyze_sentiment(t) for t in texts]
        labels = Counter(r["label"] for r in results)
        total = len(results)
        return {
            "total": total,
            "positive": labels.get("positive", 0),
            "neutral": labels.get("neutral", 0),
            "negative": labels.get("negative", 0),
            "positive_ratio": round(labels.get("positive", 0) / total * 100, 1) if total else 0,
            "negative_ratio": round(labels.get("negative", 0) / total * 100, 1) if total else 0,
            "details": results
        }

    def extract_keywords_from_notes(
        self, notes: List[Dict], field: str = "desc", top_n: int = 50
    ) -> List[Tuple[str, int]]:
        """从笔记中提取关键词"""
        texts = []
        for n in notes:
            t = n.get("title", "") or ""
            d = n.get(field, "") or ""
            texts.append(f"{t} {d}")
        return self.get_word_frequency(texts, top_n=top_n)

    def extract_keywords_from_comments(
        self, comments: List[Dict], top_n: int = 50
    ) -> List[Tuple[str, int]]:
        """从评论中提取关键词"""
        texts = [c.get("content", "") or "" for c in comments]
        return self.get_word_frequency(texts, top_n=top_n)

    def analyze_notes_sentiment(self, notes: List[Dict]) -> Dict[str, Any]:
        """分析笔记描述的情感"""
        texts = [f"{n.get('title', '')} {n.get('desc', '')}" for n in notes]
        return self.batch_sentiment(texts)

    def analyze_comments_sentiment(self, comments: List[Dict]) -> Dict[str, Any]:
        """分析评论的情感"""
        texts = [c.get("content", "") or "" for c in comments]
        return self.batch_sentiment(texts)

    def extract_hashtags(self, notes: List[Dict]) -> List[Tuple[str, int]]:
        """提取话题标签"""
        all_tags = []
        for n in notes:
            # 从 tag_list 字段
            tag_str = n.get("tag_list", "") or ""
            tags = [t.strip() for t in tag_str.split(",") if t.strip()]
            all_tags.extend(tags)
            # 从描述中提取 #话题
            desc = n.get("desc", "") or ""
            hashtags = re.findall(r'#([^#\s]+)', desc)
            all_tags.extend(hashtags)
        return Counter(all_tags).most_common(30)

    def get_text_length_stats(self, notes: List[Dict]) -> Dict[str, Any]:
        """标题/描述长度统计"""
        title_lens = [len(n.get("title", "") or "") for n in notes]
        desc_lens = [len(n.get("desc", "") or "") for n in notes]

        def stats(lst):
            if not lst:
                return {}
            return {
                "avg": round(sum(lst) / len(lst), 1),
                "max": max(lst),
                "min": min(lst),
            }

        return {
            "title_length": stats(title_lens),
            "desc_length": stats(desc_lens)
        }
