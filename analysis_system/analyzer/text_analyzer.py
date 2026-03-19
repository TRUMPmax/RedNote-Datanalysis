from __future__ import annotations

import os
import re
from collections import Counter
from typing import Any, Dict, List, Tuple

from .common import clean_text, extract_hashtags


DEFAULT_STOP_WORDS = {
    "我们",
    "你们",
    "他们",
    "她们",
    "自己",
    "一个",
    "这个",
    "那个",
    "还是",
    "就是",
    "因为",
    "所以",
    "而且",
    "已经",
    "没有",
    "真的",
    "感觉",
    "觉得",
    "可以",
    "现在",
    "以后",
    "之前",
    "之后",
    "一下",
    "一下子",
    "哈哈",
    "姐妹",
    "阿嬷",
    "奶奶",
    "爷爷",
    "视频",
    "笔记",
    "小红书",
    "评论",
    "点赞",
    "收藏",
    "分享",
}

POSITIVE_WORDS = {
    "喜欢",
    "高级",
    "优雅",
    "好看",
    "漂亮",
    "舒服",
    "温暖",
    "治愈",
    "精致",
    "松弛",
    "时尚",
    "优质",
    "值得",
    "推荐",
    "温柔",
    "可爱",
    "幸福",
    "开心",
    "惊艳",
    "实用",
    "舒服",
}

NEGATIVE_WORDS = {
    "不好",
    "失望",
    "难受",
    "难看",
    "浪费",
    "糟糕",
    "崩溃",
    "后悔",
    "问题",
    "不值",
    "贵",
    "尴尬",
    "焦虑",
    "担心",
    "麻烦",
}


def _try_import_jieba():
    try:
        import jieba

        return jieba
    except ImportError:
        return None


def _simple_tokenize(text: str) -> List[str]:
    tokens: List[str] = []
    for fragment in re.findall(r"[\u4e00-\u9fffA-Za-z0-9]{2,}", text):
        tokens.append(fragment)
        if re.fullmatch(r"[\u4e00-\u9fff]{3,}", fragment):
            for index in range(len(fragment) - 1):
                tokens.append(fragment[index : index + 2])
    return tokens


class TextAnalyzer:
    def __init__(self, stop_words_file: str | None = None):
        self.stop_words = set(DEFAULT_STOP_WORDS)
        if stop_words_file and os.path.exists(stop_words_file):
            try:
                with open(stop_words_file, "r", encoding="utf-8") as file_obj:
                    for line in file_obj:
                        word = clean_text(line)
                        if word:
                            self.stop_words.add(word)
            except Exception:
                pass

        self.jieba = _try_import_jieba()
        if self.jieba:
            try:
                self.jieba.setLogLevel(self.jieba.logging.WARN)
            except Exception:
                pass
            for word in POSITIVE_WORDS | NEGATIVE_WORDS:
                self.jieba.add_word(word)

    def tokenize(self, text: str) -> List[str]:
        content = clean_text(text)
        if not content:
            return []
        content = re.sub(r"[^\u4e00-\u9fffA-Za-z0-9#]+", " ", content)
        if self.jieba:
            words = self.jieba.lcut(content)
        else:
            words = _simple_tokenize(content)
        return [
            word.strip()
            for word in words
            if len(word.strip()) >= 2
            and word.strip() not in self.stop_words
            and not word.strip().isdigit()
        ]

    def get_word_frequency(self, texts: List[str], top_n: int = 30) -> List[Tuple[str, int]]:
        counter = Counter()
        for text in texts:
            counter.update(self.tokenize(text))
        return counter.most_common(top_n)

    def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        tokens = set(self.tokenize(text))
        positives = sorted(tokens & POSITIVE_WORDS)
        negatives = sorted(tokens & NEGATIVE_WORDS)
        pos_score = len(positives)
        neg_score = len(negatives)

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
            "positive_words": positives,
            "negative_words": negatives,
        }

    def batch_sentiment(self, texts: List[str]) -> Dict[str, Any]:
        details = [self.analyze_sentiment(text) for text in texts if clean_text(text)]
        labels = Counter(item["label"] for item in details)
        total = len(details)
        return {
            "total": total,
            "positive": labels.get("positive", 0),
            "neutral": labels.get("neutral", 0),
            "negative": labels.get("negative", 0),
            "positive_ratio": round(labels.get("positive", 0) / total * 100, 2) if total else 0,
            "negative_ratio": round(labels.get("negative", 0) / total * 100, 2) if total else 0,
            "details": details[:20],
        }

    def extract_keywords_from_notes(self, notes: List[Dict[str, Any]], top_n: int = 30) -> List[Tuple[str, int]]:
        texts = [f"{note.get('title', '')} {note.get('desc', '')}" for note in notes]
        return self.get_word_frequency(texts, top_n=top_n)

    def extract_keywords_from_comments(self, comments: List[Dict[str, Any]], top_n: int = 30) -> List[Tuple[str, int]]:
        texts = [comment.get("content", "") for comment in comments]
        return self.get_word_frequency(texts, top_n=top_n)

    def analyze_notes_sentiment(self, notes: List[Dict[str, Any]]) -> Dict[str, Any]:
        texts = [f"{note.get('title', '')} {note.get('desc', '')}" for note in notes]
        return self.batch_sentiment(texts)

    def analyze_comments_sentiment(self, comments: List[Dict[str, Any]]) -> Dict[str, Any]:
        texts = [comment.get("content", "") for comment in comments]
        return self.batch_sentiment(texts)

    def extract_hashtags(self, notes: List[Dict[str, Any]]) -> List[Tuple[str, int]]:
        counter = Counter()
        for note in notes:
            tags = note.get("tag_list", []) or []
            counter.update(clean_text(tag) for tag in tags if clean_text(tag))
            counter.update(extract_hashtags(note.get("title", "")))
            counter.update(extract_hashtags(note.get("desc", "")))
        return counter.most_common(30)

    def get_text_length_stats(self, notes: List[Dict[str, Any]]) -> Dict[str, Any]:
        title_lengths = [len(clean_text(note.get("title"))) for note in notes if clean_text(note.get("title"))]
        desc_lengths = [len(clean_text(note.get("desc"))) for note in notes if clean_text(note.get("desc"))]

        def _stats(values: List[int]) -> Dict[str, Any]:
            if not values:
                return {"avg": 0, "max": 0, "min": 0}
            return {
                "avg": round(sum(values) / len(values), 2),
                "max": max(values),
                "min": min(values),
            }

        return {
            "title_length": _stats(title_lengths),
            "desc_length": _stats(desc_lengths),
        }
