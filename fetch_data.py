#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股行情数据获取脚本 (akshare 完全版)
完全免费，无需 Token，无需注册
每天收盘后运行，获取全A股数据并整合申万三级行业，生成 data.js
"""

import os
import sys
import json
import time
import logging
import io
from datetime import datetime

import re
import requests
import pandas as pd
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from sw_industry_map_fixed import lookup as sw_lookup

# ============================================================
# 配置区域
# ============================================================

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), f"data_{datetime.now().strftime('%H%M%S')}.js")
FINAL_FILE = os.path.join(os.path.dirname(__file__), "data.js")
STOCK_LIST_CACHE = os.path.join(os.path.dirname(__file__), "stock_list_cache.json")
SW_INDUSTRY_CACHE = os.path.join(os.path.dirname(__file__), "sw_industry_cache.json")
CACHE_MAX_AGE_DAYS = 7  # 股票列表��存有效期（天），每周更新一次

LOG_FILE = os.path.join(os.path.dirname(__file__), f"fetch_data_{datetime.now().strftime('%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
SW_EXCEL_URL = "https://www.swsresearch.com/swindex/pdf/SwClass2021/StockClassifyUse_stock.xls"


# ============================================================
# 工具函数
# ============================================================

def safe_float(value, default=0.0):
    try:
        if value is None or (isinstance(value, str) and value.strip() in ("", "-", "--")):
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


def format_code(code_str):
    """格式化股票代码为纯6位数字"""
    if isinstance(code_str, (int, float)):
        return str(int(code_str)).zfill(6)
    code_str = str(code_str).strip()
    for suffix in (".SH", ".SZ", ".BJ", ".sh", ".sz", ".bj"):
        if code_str.endswith(suffix):
            code_str = code_str[:-len(suffix)]
    return code_str.zfill(6)


# ============================================================
# 数据获取
# ============================================================

def get_active_stock_codes_eastmoney():
    """获取在巿A股代码列表（优先用缓存，每月更新一次）"""
    logger.info("步骤 0/4: 获取在巿股票列表（过滤退市股）...")

    # 检查缓存是否有效
    if os.path.exists(STOCK_LIST_CACHE):
        cache_mtime = os.path.getmtime(STOCK_LIST_CACHE)
        cache_age_days = (time.time() - cache_mtime) / 86400
        if cache_age_days < CACHE_MAX_AGE_DAYS:
            try:
                with open(STOCK_LIST_CACHE, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                codes = set(cache['codes'])
                cache_date = cache.get('date', 'unknown')
                logger.info(f"使用缓存股票列表（{cache_date}，{len(codes)} 只，缓存 {cache_age_days:.0f} 天前）")
                return codes
            except Exception as e:
                logger.warning(f"缓存读取失败: {e}，将重新从东方财富获取")

    # 缓存不存在或已过期，从东方财富获取
    logger.info("缓存失效或不存在，从东方财富获取最新股票列表...")
    try:
        import akshare as ak
        df = ak.stock_info_a_code_name()
        codes = set(df['code'].tolist())
        logger.info(f"东方财富在巿A股: {len(codes)} 只")

        # 保存缓存
        try:
            cache_data = {
                'date': datetime.now().strftime('%Y-%m-%d'),
                'source': '东方财富 stock_info_a_code_name',
                'count': len(codes),
                'codes': sorted(list(codes))
            }
            with open(STOCK_LIST_CACHE, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False)
            logger.info(f"股票列表缓存已更新: {STOCK_LIST_CACHE}")
        except Exception as e:
            logger.warning(f"缓存保存失败: {e}")

        return codes
    except Exception as e:
        logger.warning(f"东方财富列表获取失败: {e}")

        # 回退：尝试用旧缓存
        if os.path.exists(STOCK_LIST_CACHE):
            try:
                with open(STOCK_LIST_CACHE, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                codes = set(cache['codes'])
                logger.info(f"回退使用旧缓存: {len(codes)} 只")
                return codes
            except Exception:
                pass

        logger.warning("无可用股票列表，将跳过退市过滤")
        return None


def get_spot_data(sw_stock_codes=None, em_active_codes=None):
    """获取全A股实时行情（价格、涨跌幅、总市值(字段45)、PE(TTM,字段39)、年初至今涨跌幅(字段62)）- 使用腾讯qt API
    sw_stock_codes: 可选，从申万Excel获取的股票代码列表用于确定市场(sh/sz)
    em_active_codes: 可选，东方财富在巿股票代码集合，用于过滤退市股
    """
    logger.info("步骤 2/4: 获取全A股实时行情（腾讯API）...")

    import requests as req

    # 首先通过新浪API获取全部股票代码列表
    logger.info("  获取股票列表...")
    QT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "http://finance.qq.com/",
    }

    # 使用新浪API获取全部A股列表
    all_codes = []
    # A股市场: sh(沪市主板), sz(深市主板/创业板)
    # 先尝试获取常见的代码范围
    
    # 直接使用申万Excel代码列表（跳过可能失效的东方财富API）
    if sw_stock_codes:
        for code in sw_stock_codes:
            code = str(code).strip().zfill(6)
            if code.startswith("900"):
                continue  # 跳过沪市B股(900901-900957)
            if code.startswith("200"):
                continue  # 跳过深市B股(200001-200992)
            if code.startswith(("0", "3")):
                all_codes.append(f"sz{code}")
            elif code.startswith(("92", "4", "8")):
                all_codes.append(f"bj{code}")    # 92/4/8开头 = 北交所
            elif code.startswith("6"):
                all_codes.append(f"sh{code}")     # 6开头 = 沪市主板+科创板(688)

    if not all_codes:
        logger.error("无法获取股票代码列表")
        return pd.DataFrame()

    # 用东方财富在巿列表过滤退市股（全部市场统一过滤）
    if em_active_codes:
        filtered_codes = []
        removed = 0
        for qt_code in all_codes:
            raw_code = qt_code[2:]  # 去掉sh/sz/bj前缀，纯6位数字
            if raw_code in em_active_codes:
                filtered_codes.append(qt_code)
            else:
                removed += 1
        logger.info(f"  过滤前: {len(all_codes)} 只, 剔除退市股: {removed} 只, 过滤后: {len(filtered_codes)} 只")
        all_codes = filtered_codes
    else:
        logger.info(f"  共 {len(all_codes)} 只股票（未使用东方财富过滤）")

    # 使用腾讯qt API批量获取行情 (每批50只)
    BATCH_SIZE = 50
    QT_URL = "http://qt.gtimg.cn/q="
    all_rows = []
    total_batches = (len(all_codes) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_idx in range(0, len(all_codes), BATCH_SIZE):
        batch = all_codes[batch_idx:batch_idx + BATCH_SIZE]
        batch_num = batch_idx // BATCH_SIZE + 1
        
        url = QT_URL + ",".join(batch)
        try:
            r = req.get(url, headers=QT_HEADERS, timeout=20)
            r.encoding = "gbk"
            lines = r.text.strip().split("\n")
        except Exception as e:
            logger.warning(f"  第{batch_num}批请求失败: {e}")
            continue

        for line in lines:
            line = line.strip()
            if not line or "=" not in line:
                continue
            try:
                # 格式: v_sh600000="1~名称~代码~最新价~昨收~..."
                data_str = line.split('="', 1)[1].rstrip('";')
                fields = data_str.split("~")
                if len(fields) < 45:
                    continue

                code = fields[2].strip()
                name = fields[1].strip()
                price = safe_float(fields[3])
                change_pct = safe_float(fields[32]) if len(fields) > 32 else 0
                ytd_change = safe_float(fields[62]) if len(fields) > 62 else 0  # 年初至今涨跌幅(字段62, 非字段31涨跌额)
                cap = safe_float(fields[45]) if len(fields) > 45 else 0  # 总市值=字段45(亿), 字段44是流通市值
                pe = safe_float(fields[39]) if len(fields) > 39 else 0  # PE(TTM) = 字段39

                if code and name:
                    # 过滤无效行情（退市股已由东方财富在巿列表过滤）
                    if price <= 0:
                        continue
                    all_rows.append({
                        "代码": code,
                        "名称": name,
                        "最新价": price,
                        "涨跌幅": change_pct,
                        "年初至今涨幅": ytd_change,
                        "总市值": cap,
                        "PE": pe,
                    })
            except Exception:
                continue

        if batch_num % 20 == 0:
            logger.info(f"  已处理 {batch_num}/{total_batches} 批, 累计 {len(all_rows)} 条")
        time.sleep(0.3)  # 控制频率

    df = pd.DataFrame(all_rows)
    logger.info(f"获取到 {len(df)} 只股票行情数据")
    return df


def get_sw_industry(new_codes=None):
    """获取申万行业分类（优先用缓存，仅对新股票查官网）
    缓存永久有效，不再每天更新行业分类。
    new_codes: 可选，需要查询行业的新股票代码列表
    """
    logger.info("步骤 1/3: 获取申万行业分类...")

    # 加载缓存
    industry_map = {}
    if os.path.exists(SW_INDUSTRY_CACHE):
        try:
            with open(SW_INDUSTRY_CACHE, 'r', encoding='utf-8') as f:
                industry_map = json.load(f)
            logger.info(f"加载行业缓存: {len(industry_map)} 条记录")
        except Exception as e:
            logger.warning(f"行业缓存读取失败: {e}，将重新获取全部")
            industry_map = {}

    # 检查是否有新股票需要查询
    if new_codes:
        missing = [c for c in new_codes if str(c).strip().zfill(6) not in industry_map]
    else:
        missing = []

    if not missing and industry_map:
        logger.info(f"行业缓存完整，跳过更新（{len(industry_map)} 条）")
        return industry_map

    if not industry_map:
        logger.info("行业缓存为空，从申万官网下载全部分类...")
    else:
        logger.info(f"有 {len(missing)} 只新股票需查询行业...")

    # 从申万官网下载Excel
    try:
        logger.info("正在从申万官网下载分类数据...")
        r = requests.get(SW_EXCEL_URL, verify=False, timeout=60, headers=HEADERS)
        df = pd.read_excel(io.BytesIO(r.content), dtype={"股票代码": "str", "行业代码": "str"})
        logger.info(f"下载完成: {len(df)} 条记录")

        # 取每只股票的最新行业分类（按计入日期，即分类生效日期）
        df_sorted = df.sort_values(["股票代码", "计入日期"], ascending=[True, False])
        latest = df_sorted.groupby("股票代码").first().reset_index()

        # 构建映射（全量或增量）
        fresh_count = 0
        for _, row in latest.iterrows():
            stock_code = format_code(row["股票代码"])
            # 如果是增量模式，只更新缺失的
            if industry_map and stock_code not in missing:
                continue
            ind_code = str(row["行业代码"]).strip()
            l1, l2, l3 = sw_lookup(ind_code)
            industry_map[stock_code] = {"L1": l1 or "", "L2": l2 or "", "L3": l3 or ""}
            fresh_count += 1

        if industry_map:
            unmapped = sum(1 for v in industry_map.values() if not v['L1'] and not v['L2'] and not v['L3'])
            logger.info(f"行业映射: 新增/更新 {fresh_count}, 总计 {len(industry_map)}, 无行业 {unmapped}")

        # 保存缓存
        try:
            with open(SW_INDUSTRY_CACHE, 'w', encoding='utf-8') as f:
                json.dump(industry_map, f, ensure_ascii=False)
            logger.info(f"行业缓存已更新: {SW_INDUSTRY_CACHE}")
        except Exception as e:
            logger.warning(f"行业缓存保存失败: {e}")

        return industry_map

    except Exception as e:
        logger.warning(f"申万官网下载失败: {e}")
        if industry_map:
            logger.info(f"回退使用缓存行业数据（{len(industry_map)} 条）")
            return industry_map
        logger.error("无行业数据可用")
        return {}


def get_concept_map():
    """从本地概念Excel文件读取股票→概念标签映射"""
    concept_file = os.path.join(os.path.dirname(__file__), "概念.xlsx")
    logger.info("步骤 3/4: 读取概念标签...")

    if not os.path.exists(concept_file):
        logger.warning(f"  概念文件不存在: {concept_file}，跳过概念数据")
        return {}

    try:
        import pandas as pd
        df = pd.read_excel(concept_file, dtype={'代码': 'str'})
        logger.info(f"  概念文件: {len(df)} 条记录")
    except Exception as e:
        logger.warning(f"  概念文件读取失败: {e}")
        return {}

    stock_concepts = {}
    total_concepts = 0
    for _, row in df.iterrows():
        raw_code = str(row.get('代码', '')).strip()
        # 去掉后缀 .SH/.SZ/.BJ
        code = raw_code.replace('.SH', '').replace('.SZ', '').replace('.BJ', '').strip().zfill(6)
        if len(code) != 6:
            continue
        concept_str = str(row.get('概念', '')).strip()
        if not concept_str:
            continue
        # 逗号分隔，清理空格和下划线结尾
        concepts = []
        for c in concept_str.split(','):
            c = c.strip().rstrip('_')
            if c:
                concepts.append(c)
        if concepts:
            stock_concepts[code] = concepts
            total_concepts += len(concepts)

    logger.info(f"  概念映射完成: {len(stock_concepts)} 只股票有概念, 共 {total_concepts} 个概念标签, {len(set(c for cl in stock_concepts.values() for c in cl))} 个独立概念")
    return stock_concepts


def get_main_business(stock_codes):
    """通过akshare获取股票主营构成（并发）"""
    logger.info("步骤 4/4: 获取主营构成（akshare并发）...")

    # 构建带前缀的代码列表
    codes_with_prefix = []
    for code in stock_codes:
        code = str(code).strip().zfill(6)
        if code.startswith('6'):
            codes_with_prefix.append(f"SH{code}")
        elif code.startswith(('0', '3')):
            codes_with_prefix.append(f"SZ{code}")
        elif code.startswith(('4', '8', '92')):
            codes_with_prefix.append(f"BJ{code}")

    logger.info(f"  共 {len(codes_with_prefix)} 只股票待获取")

    biz_map = {}
    done = 0
    errors = 0
    t0 = time.time()

    def fetch_one(symbol):
        try:
            import akshare as ak
            df = ak.stock_zygc_em(symbol=symbol)
            if len(df) == 0:
                return symbol, ""
            # 只取2025年年报：报告日期以"2025"开头
            df_2025 = df[df['报告日期'].astype(str).str.startswith('2025')]
            if len(df_2025) == 0:
                return symbol, ""
            df = df_2025
            # 取最新报告期
            latest_date = df['报告日期'].max()
            df = df[df['报告日期'] == latest_date]
            # 优先"按产品分类"，更直观；无则回退"按行业分类"
            for cat in ['按产品分类', '按行业分类']:
                subset = df[df['分类类型'] == cat]
                if len(subset) > 0:
                    # 按收入比例降序排列
                    subset = subset.sort_values('收入比例', ascending=False)
                    items = []
                    for _, r in subset.iterrows():
                        pct = r['收入比例']
                        if pct > 0:
                            items.append(f"{r['主营构成']}({pct:.0%})")
                    if items:
                        return symbol, ";".join(items)
            return symbol, ""
        except Exception:
            return symbol, ""

    with ThreadPoolExecutor(max_workers=15) as ex:
        futures = [ex.submit(fetch_one, s) for s in codes_with_prefix]
        for f in as_completed(futures):
            symbol, biz = f.result()
            done += 1
            code = symbol[2:]  # 去掉SH/SZ/BJ前缀
            if biz:
                biz_map[code] = biz
            else:
                errors += 1
            if done % 500 == 0:
                elapsed = time.time() - t0
                logger.info(f"  已处理 {done}/{len(codes_with_prefix)}, "
                           f"有效 {len(biz_map)}, 耗时 {elapsed:.0f}s")

    elapsed = time.time() - t0
    logger.info(f"  主营构成完成: {len(biz_map)} 只有数据, {errors} 只无数据, 耗时 {elapsed:.0f}s")
    return biz_map


# ============================================================
# 数据整合
# ============================================================

def get_roe_and_forecast():
    """获取ROE(最新报告期)和2026年盈利预测(EPS一致预期)数据"""
    logger.info("步骤 5/5: 获取ROE和盈利预测数据...")

    # --- ROE from stock_yjbb_em (批量) ---
    roe_map = {}
    try:
        import akshare as ak
        df = ak.stock_yjbb_em(date='20251231')
        for _, row in df.iterrows():
            code = str(row.get('股票代码', '')).strip().zfill(6)
            roe = safe_float(row.get('净资产收益率', 0))
            if code and roe != 0:
                roe_map[code] = roe
        logger.info(f"  ROE数据: {len(roe_map)} 只股票")
    except Exception as e:
        logger.warning(f"  ROE获取失败: {e}")

    # --- EPS forecast from eastmoney datacenter (批量) ---
    forecast_map = {}
    try:
        import requests as req
        url = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
        page = 1
        total_fetched = 0
        while True:
            params = {
                'reportName': 'RPT_WEB_RESPREDICT',
                'pageNumber': page,
                'pageSize': 1000,
                'columns': 'ALL',
            }
            r = req.get(url, params=params, timeout=20, headers={'User-Agent': 'Mozilla/5.0'})
            data = r.json()
            if not data.get('success') or not data.get('result'):
                break
            rows = data['result']['data']
            total_count = data['result'].get('count', 0)
            if page == 1:
                logger.info(f"  盈利预测: 共 {total_count} 只股票有机构预测")
            for row in rows:
                code = str(row.get('SECURITY_CODE', '')).strip().zfill(6)
                eps1 = safe_float(row.get('EPS1', 0))  # 2025 actual (mark=A)
                eps2 = safe_float(row.get('EPS2', 0))  # 2026 forecast (mark=E)
                if code and eps2 > 0:
                    yoy = 0
                    if eps1 > 0:
                        yoy = round((eps2 - eps1) / eps1 * 100, 2)
                    forecast_map[code] = {
                        'eps1': eps1,
                        'eps2': eps2,
                        'yoy': yoy,
                    }
            total_fetched += len(rows)
            if len(rows) == 0 or total_fetched >= total_count:
                break
            page += 1
        logger.info(f"  盈利预测数据: {len(forecast_map)} 只股票")
    except Exception as e:
        logger.warning(f"  盈利预测获取失败: {e}")

    return roe_map, forecast_map


def get_profit_forecast_ths(forecast_map):
    """通过同花顺API获取2026年一致预测净利润（亿），仅对有EPS预测的股票"""
    logger.info("步骤 5.5/5: 获取预测净利润（同花顺）...")
    codes_to_fetch = list(forecast_map.keys())
    logger.info(f"  待获取: {len(codes_to_fetch)} 只股票")

    profit_map = {}
    done = 0
    errors = 0
    t0 = time.time()

    def fetch_one(code):
        try:
            import akshare as ak
            df = ak.stock_profit_forecast_ths(symbol=code, indicator='预测年报净利润')
            # 找2026年行（年度列是字符串）
            row = df[df['年度'].astype(str) == '2026']
            if len(row) > 0:
                mean_val = float(row.iloc[0]['均值'])
                return code, round(mean_val, 2)
            return code, 0
        except Exception:
            return code, 0

    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = [ex.submit(fetch_one, c) for c in codes_to_fetch]
        for f in as_completed(futures):
            code, profit = f.result()
            done += 1
            if profit > 0:
                profit_map[code] = profit
            else:
                errors += 1
            if done % 200 == 0:
                elapsed = time.time() - t0
                logger.info(f"  已处理 {done}/{len(codes_to_fetch)}, 有效 {len(profit_map)}, 耗时 {elapsed:.0f}s")

    elapsed = time.time() - t0
    logger.info(f"  预测净利润完成: {len(profit_map)} 只有数据, {errors} 只无数据, 耗时 {elapsed:.0f}s")
    return profit_map


def integrate_data(spot_df, industry_map, concept_map, biz_map, roe_map=None, forecast_map=None, profit_forecast_map=None):
    """整合行情、行业、概念和主营数据"""
    logger.info("整合数据...")
    result = []

    for _, row in spot_df.iterrows():
        try:
            code = format_code(row.get("代码", ""))
            name = str(row.get("名称", "")).strip()
            price = round(safe_float(row.get("最新价", 0)), 2)
            change = round(safe_float(row.get("涨跌幅", 0)), 2)
            ytd = round(safe_float(row.get("年初至今涨幅", 0)), 2)

            # 总市值: 字段45(总市值,亿), 字段44是流通市值
            cap_raw = safe_float(row.get("总市值", 0))
            cap = round(cap_raw, 2)

            # PE(TTM): 腾讯API字段39
            pe_raw = safe_float(row.get("PE", 0))
            pe = round(pe_raw, 2)

            # 行业分类
            ind = industry_map.get(code, {"L1": "", "L2": "", "L3": ""})

            # 概念标签: 取前5个，用逗号分隔
            concepts = concept_map.get(code, [])
            concept_str = "、".join(concepts[:8]) if concepts else ""

            # 主营构成
            biz = biz_map.get(code, "")

            # ROE
            roe = roe_map.get(code, 0) if roe_map else 0

            # 2026年盈利预测（一致预期EPS，来自东方财富）
            forecast = forecast_map.get(code, {}) if forecast_map else {}
            eps2 = forecast.get('eps2', 0)
            forecast_yoy = forecast.get('yoy', 0)
            forecast_eps = round(eps2, 2) if eps2 > 0 else 0

            # 2026年预测净利润（同花顺一致预期，亿元）
            forecast_profit = profit_forecast_map.get(code, 0) if profit_forecast_map else 0

            result.append({
                "industry1": ind["L1"],
                "industry2": ind["L2"],
                "industry3": ind["L3"],
                "code": code,
                "name": name,
                "concept": concept_str,
                "biz": biz,
                "price": price,
                "change": change,
                "ytd": ytd,
                "cap": cap,
                "pe": pe,
                "roe": round(roe, 2),
                "forecastProfit": forecast_profit,
                "forecastEps": forecast_eps,
                "forecastYoy": forecast_yoy,
            })
        except Exception:
            pass

    # 按申万三级行业排序
    result.sort(key=lambda x: (x["industry3"] or "zzzzz", x["code"]))
    logger.info(f"整合完成: {len(result)} 条记录")
    return result


# ============================================================
# 输出
# ============================================================

def write_data_js(data, update_time):
    json_str = json.dumps(data, ensure_ascii=False, indent=2)

    content = f"""// A股行情数据
// 更新时间: {update_time}
// 自动生成，请勿手动编辑
// 数据来源: 沪深京交易所 + 申万宏源研究所(行业分类) + 本地概念Excel + 东方财富(主营:2025年报,ROE,盈利预测)
// 请注意核实数据，仅供学习参考，不构成投资建议

const stockData = {json_str};

const DATA_UPDATE_TIME = "{update_time}";
"""

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(content)

    size_mb = os.path.getsize(OUTPUT_FILE) / 1024 / 1024
    logger.info(f"数据已写入临时文件: {OUTPUT_FILE} ({size_mb:.1f} MB)")

    # 尝试替换正式文件
    try:
        import shutil
        shutil.move(OUTPUT_FILE, FINAL_FILE)
        logger.info(f"已替换正式文件: {FINAL_FILE}")
    except PermissionError:
        logger.warning(f"无法替换 {FINAL_FILE}（文件被占用），数据保存在 {OUTPUT_FILE}")
        logger.warning("请关闭占用该文件的程序后，手动重命名 data_tmp.js 为 data.js")
    except Exception as e:
        logger.warning(f"替换文件时出错: {e}")


# ============================================================
# 主流程
# ============================================================

def main():
    start_time = time.time()
    logger.info("=" * 50)
    logger.info("A股行情数据获取脚本 开始运行")
    logger.info(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)

    # Step 0: 获取在巿股票列表（缓存，用于过滤退市股和新股检测）
    try:
        em_codes = get_active_stock_codes_eastmoney()
    except Exception as e:
        logger.warning(f"股票列表获取失败: {e}，将跳过退市过滤")
        em_codes = None

    # Step 1: 申万行业分类（优先缓存，仅对新股票查官网）
    try:
        new_check_codes = list(em_codes) if em_codes else None
        industry_map = get_sw_industry(new_codes=new_check_codes)
        sw_codes = list(industry_map.keys())
    except Exception as e:
        logger.error(f"行业分类获取失败: {e}")
        industry_map = {}
        sw_codes = None

    # Step 2: 实时行情（使用腾讯API，过滤退市股）
    try:
        spot_df = get_spot_data(sw_stock_codes=sw_codes, em_active_codes=em_codes)
        if len(spot_df) == 0:
            logger.error("未获取到任何行情数据")
            sys.exit(1)
    except Exception as e:
        logger.error(f"行情获取失败: {e}")
        sys.exit(1)

    # Step 3: 概念标签（同花顺爬取）
    try:
        concept_map = get_concept_map()
    except Exception as e:
        logger.warning(f"概念获取失败: {e}，将跳过概念数据")
        concept_map = {}

    # Step 4: 主营构成（akshare并发获取）
    # 用行情数据中的股票代码
    spot_codes = [format_code(c) for c in spot_df["代码"].tolist()]
    try:
        biz_map = get_main_business(spot_codes)
    except Exception as e:
        logger.warning(f"主营构成获取失败: {e}，将跳过主营数据")
        biz_map = {}

    # Step 5: ROE和盈利预测
    try:
        roe_map, forecast_map = get_roe_and_forecast()
    except Exception as e:
        logger.warning(f"ROE和盈利预测获取失败: {e}")
        roe_map, forecast_map = {}, {}

    # Step 5.5: 同花顺预测净利润
    profit_forecast_map = {}
    if forecast_map:
        try:
            profit_forecast_map = get_profit_forecast_ths(forecast_map)
        except Exception as e:
            logger.warning(f"预测净利润获取失败: {e}")

    # Step 6: 整合
    stock_data = integrate_data(spot_df, industry_map, concept_map, biz_map, roe_map, forecast_map, profit_forecast_map)

    # Step 6: 写入
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    write_data_js(stock_data, now)

    # 统计
    price_ok = sum(1 for s in stock_data if s["price"] > 0)
    industry_ok = sum(1 for s in stock_data if s["industry1"])
    cap_ok = sum(1 for s in stock_data if s["cap"] > 0)
    concept_ok = sum(1 for s in stock_data if s.get("concept"))
    biz_ok = sum(1 for s in stock_data if s.get("biz"))
    pe_ok = sum(1 for s in stock_data if s.get("pe", 0) > 0)
    roe_ok = sum(1 for s in stock_data if s.get("roe", 0) > 0)
    forecast_ok = sum(1 for s in stock_data if s.get("forecastEps", 0) > 0)
    profit_ok = sum(1 for s in stock_data if s.get("forecastProfit", 0) > 0)

    elapsed = time.time() - start_time
    logger.info("=" * 50)
    logger.info("运行完成!")
    logger.info(f"  股票总数: {len(stock_data)}")
    logger.info(f"  有行情: {price_ok}  |  有行业: {industry_ok}  |  有市值: {cap_ok}")
    logger.info(f"  有PE: {pe_ok}  |  有ROE: {roe_ok}  |  有预测EPS: {forecast_ok}  |  有预测净利: {profit_ok}")
    logger.info(f"  有概念: {concept_ok}  |  有主营: {biz_ok}")
    logger.info(f"  总耗时: {elapsed:.1f} 秒")
    logger.info(f"  输出: {OUTPUT_FILE}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
