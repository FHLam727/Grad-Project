# 澳門活動監察系統

爬取澳門六大運營商（永利、金沙、銀河、美高梅、新濠、葡京）及政府旅遊局的 XHS、微博、Instagram、Facebook 官方帳號，自動分類活動並透過介面展示。

---

## 前置條件

- 已安裝並設定好 [MediaCrawler](https://github.com/NanmiCoder/MediaCrawler)（XHS / 微博）
- 已在 MediaCrawler 完成 XHS 同微博掃碼登入
- 擁有有餘額的 [DeepSeek API Key](https://platform.deepseek.com)
- 擁有 [Apify API Token](https://apify.com)（IG / FB 爬取）
- 擁有 [阿里雲 DashScope API Key](https://dashscope.console.aliyun.com)（圖片 OCR）

---

## 安裝步驟

### 1. Clone repo

```bash
git clone https://github.com/FHLam727/Grad-Project.git
```

### 2. 將核心檔案複製入 MediaCrawler 根目錄

```bash
cp bridge.py db_manager.py task_manager.py post_normalizer.py \
   media_analyzer.py trad_simp.py \
   operation_panel.html macau_analytics.db \
   /path/to/MediaCrawler/
```

### 3. 覆蓋 MediaCrawler 修改過的檔案

```bash
cp mediacrawler_patches/config/base_config.py /path/to/MediaCrawler/config/
cp mediacrawler_patches/media_platform/weibo/client.py /path/to/MediaCrawler/media_platform/weibo/
cp mediacrawler_patches/media_platform/weibo/core.py /path/to/MediaCrawler/media_platform/weibo/
```

**改動摘要：**

| 檔案 | 改動內容 |
|------|----------|
| `config/base_config.py` | `CRAWLER_MAX_NOTES_COUNT` 由 `15` 改成 `50`；`ENABLE_GET_IMAGES` 改成 `True` |
| `media_platform/weibo/client.py` | `get_all_notes_by_creator()` 加 `max_count` 上限 |
| `media_platform/weibo/core.py` | 爬取時傳入 `config.CRAWLER_MAX_NOTES_COUNT`，改用固定 sleep interval |

### 4. 安裝額外依賴

```bash
pip install -r requirements_extra.txt
pip install openai python-dotenv httpx pillow opencc-python-reimplemented
```

### 5. 設定 API Keys

喺 MediaCrawler 根目錄建立 `.env` 檔案：

```
DASHSCOPE_API_KEY=sk-xxxx        # 阿里雲 DashScope（圖片OCR）
```

打開 `bridge.py`，換成自己的 DeepSeek Key：

```python
client = OpenAI(api_key="你的KEY", base_url="https://api.deepseek.com")
```

打開 `task_manager.py`，換成自己的 Apify Token：

```python
APIFY_TOKEN = "你的TOKEN"
```

---

## 使用方式

### 啟動主介面

```bash
python bridge.py
```

然後直接用瀏覽器打開 `operation_panel.html`

### 圖片 OCR 分析（獨立執行）

```bash
# 首次執行：分析所有未處理帖文
python media_analyzer.py --limit 100

# 只做 dry-run 睇會處理咩
python media_analyzer.py --dry-run

# 清除失敗記錄重新跑
python media_analyzer.py --reset-failed --limit 100
```

建議每次爬完新帖後執行一次 `media_analyzer.py`，補充圖片文字資料。

### 單獨重爬某個平台

```bash
# 只重爬微博（唔影響其他平台）
python -c "
from task_manager import _crawl_platform
_crawl_platform('wb', ['wynn','sands','galaxy','mgm','melco','sjm','government'], '')
"

# 只重爬單一帳號（例如微博 UID 5577774461）
python main.py --platform wb --type creator --creator_id 5577774461 --headless 0
# 爬完後手動入庫
python -c "
from db_manager import ingest_crawler_data
ingest_crawler_data('data/weibo/json/creator_contents_5577774461.json', 'weibo', '', operator='melco')
"
```

### 分類誤判測試工具

```bash
python classifier_tester.py
```

瀏覽器開 `http://localhost:8765`

---

## 檔案說明

| 檔案 | 功能 |
|------|------|
| `bridge.py` | FastAPI server，處理前端請求、call DeepSeek、返回活動 cards |
| `db_manager.py` | 所有 DB 操作：入庫、查詢、日期解析、backfill |
| `task_manager.py` | 控制 MediaCrawler（XHS/微博）及 Apify（IG/FB）爬蟲 |
| `post_normalizer.py` | 將各平台 raw post 標準化入 posts_* 表（去emoji、繁體化、清洗）|
| `media_analyzer.py` | 圖片 OCR 工具，用 Qwen-VL 提取圖片文字存入 `media_text` 欄位 |
| `trad_simp.py` | 繁簡字符互轉工具（基於 opencc） |
| `operation_panel.html` | 前端介面，選擇運營商/類別/日期範圍查看活動 |
| `classifier_tester.py` | 本地測試工具，對比規則分類同 AI 分類結果 |
| `macau_analytics.db` | SQLite 資料庫，儲存所有爬取帖文同政府活動數據 |
| `mediacrawler_patches/` | 修改過的 MediaCrawler 原始檔案，需覆蓋到對應路徑 |

---

## 數據庫結構

```
macau_events          — 原始爬取數據（raw_json、event_date、transcript）
    ↓ post_normalizer.py
posts_xhs             — XHS 標準化帖文（content、media_urls、media_text）
posts_ig              — Instagram 標準化帖文
posts_fb              — Facebook 標準化帖文
posts_weibo           — 微博標準化帖文
crawl_log             — 記錄各運營商最後爬取時間
```

### 重要欄位說明

| 欄位 | 所在表 | 說明 |
|------|--------|------|
| `content` | `posts_*` | 清洗後純文字（去emoji、去hashtag、繁體化） |
| `media_urls` | `posts_*` | 圖片/影片 URL（JSON array） |
| `media_text` | `posts_*` | Qwen-VL OCR 從圖片提取嘅文字 |
| `raw_json` | `macau_events` | 原始爬取 JSON |
| `transcript` | `macau_events` | 預留欄位（媒體轉錄） |

---

## 平台覆蓋

| 運營商 | XHS | 微博 | Instagram | Facebook |
|--------|-----|------|-----------|----------|
| 永利 Wynn | ✅ | ✅ | ✅ | ✅ |
| 金沙 Sands | ✅ | ✅ | ✅ | ✅ |
| 銀河 Galaxy | ✅ | ✅ | ✅ | ✅ |
| 美高梅 MGM | ✅ | ✅ | ✅ | ✅ |
| 新濠 Melco | ✅ | ✅ | ✅ | ✅ |
| 葡京 SJM | ✅ | ✅ | ✅ | ✅ |
| 政府旅遊局 | ✅ | ✅ | ✅ | ✅ |

---

## AI 分析流程

```
posts_* (content + media_text)
    ↓ bridge.py query_db_by_filters()
    ↓ DeepSeek 提取活動資訊
    ↓ 日期驗證、去重、繁體化
前端 operation_panel.html 顯示 cards
```

**圖片 OCR 整合邏輯（`bridge.py`）：**
- `media_text` 係從圖片自動 OCR 提取，可能含雜音
- 只有當中同時出現明確活動名稱同日期時，DeepSeek 先參考，否則忽略
- 有助補捉 description 較短但海報資訊豐富嘅帖文

---

## 常見問題

**DeepSeek 出現 402 錯誤**
→ 帳戶餘額不足，前往 [platform.deepseek.com](https://platform.deepseek.com) 充值

**爬不到 XHS 或微博**
→ 登入狀態過期，重新在 MediaCrawler 掃碼登入

**media_analyzer.py 圖片 OCR 失敗（400 Bad Request）**
→ 圖片 URL 有防盜鏈限制，代碼已用 base64 本地下載模式處理，重新跑即可
→ 如仍失敗，用 `--reset-failed` 清除記錄重試

**Paraformer 影片語音轉錄 Connection error**
→ 阿里雲 Paraformer `/audio/transcriptions` endpoint 澳門連唔上，已移除此功能
→ 影片帖文改為 OCR 封面圖提取文字

**分類全都是 experience**
→ 正常，`experience` 係預設分類（帖文冇 match 任何關鍵字時）

**posts_* 表的 media_text 全部係 NULL**
→ 需要執行 `python media_analyzer.py` 填充圖片 OCR 結果
