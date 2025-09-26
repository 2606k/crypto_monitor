import requests
import pandas as pd
import numpy as np
import os
import time
import json
import logging
import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass

# Configure logging for GitHub Actions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crypto_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ContractData:
    """Data structure for contract data."""
    symbol: str
    mark_price: float
    index_price: float
    basis: float
    basis_percent: float
    last_funding_rate: float
    oi: float
    long_short_account_ratio: float
    top_trader_account_ls_ratio: float
    top_trader_position_ls_ratio: float
    taker_buy_sell_ratio: float
    timestamp: str

class BinanceDataFetcher:
    """Binance data fetcher optimized for GitHub Actions."""

    def __init__(self):
        # 使用多个备选端点
        self.base_urls = [
            "https://fapi.binance.com",
            "https://api.binance.com",
            "https://dapi.binance.com",
            "https://api1.binance.com",
            "https://api2.binance.com",
            "https://api3.binance.com"
        ]
        
        self.current_base_url = None
        self.session = None
        self.request_timeout = 20
        self.max_retries = 3

    async def __aenter__(self):
        # 创建更宽松的连接器配置
        connector = aiohttp.TCPConnector(
            limit=30,
            limit_per_host=10,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        
        timeout = aiohttp.ClientTimeout(total=self.request_timeout, connect=10)
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive'
            }
        )
        
        await self._find_working_endpoint()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
            # 等待连接完全关闭
            await asyncio.sleep(0.1)

    async def _find_working_endpoint(self):
        """寻找可用的API端点"""
        logger.info("正在测试API端点连接...")
        
        for i, base_url in enumerate(self.base_urls):
            try:
                # 使用更简单的ping端点
                if 'fapi' in base_url:
                    test_url = f"{base_url}/fapi/v1/ping"
                else:
                    test_url = f"{base_url}/api/v3/ping"
                
                logger.info(f"测试端点 {i+1}/{len(self.base_urls)}: {base_url}")
                
                async with self.session.get(test_url, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    if response.status == 200:
                        self.current_base_url = base_url
                        logger.info(f"成功连接到: {base_url}")
                        return
                    else:
                        logger.warning(f"{base_url} 返回状态码: {response.status}")
                        
            except asyncio.TimeoutError:
                logger.warning(f"{base_url} 连接超时")
            except Exception as e:
                logger.warning(f"{base_url} 连接失败: {str(e)}")
                continue
        
        # 如果所有端点都失败，尝试直接使用第一个
        logger.warning("所有端点测试失败，尝试使用默认端点")
        self.current_base_url = self.base_urls[0]

    async def _make_request(self, endpoint: str, params: Dict = None, use_fapi: bool = True) -> Dict:
        """统一的请求方法"""
        if not self.current_base_url:
            logger.error("没有可用的API端点")
            return {}
            
        # 根据端点类型选择合适的base URL
        base_url = self.current_base_url
        if not use_fapi and 'fapi' in base_url:
            # 如果当前是fapi端点但需要普通API，尝试替换
            base_url = base_url.replace('fapi.', 'api.')
        
        for attempt in range(self.max_retries):
            try:
                url = f"{base_url}{endpoint}"
                logger.debug(f"请求URL: {url}, 参数: {params}")
                
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.debug(f"成功获取数据: {endpoint}")
                        return data
                    elif response.status == 429:
                        wait_time = (2 ** attempt) + 1
                        logger.warning(f"API限流，等待{wait_time}秒...")
                        await asyncio.sleep(wait_time)
                        continue
                    elif response.status == 403:
                        logger.error(f"API访问被禁止: {response.status}")
                        return {}
                    else:
                        error_text = await response.text()
                        logger.error(f"API请求失败: {response.status} - {error_text}")
                        
            except asyncio.TimeoutError:
                logger.warning(f"请求超时 (尝试 {attempt + 1}/{self.max_retries}): {endpoint}")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"请求异常: {e} (尝试 {attempt + 1}/{self.max_retries})")
                await asyncio.sleep(1)
        
        logger.error(f"所有重试失败: {endpoint}")
        return {}

    async def get_all_usdt_symbols(self) -> List[str]:
        """获取所有USDT永续合约交易对"""
        try:
            # 尝试期货API
            data = await self._make_request("/fapi/v1/exchangeInfo", use_fapi=True)
            
            if not data or 'symbols' not in data:
                logger.warning("期货API失败，尝试现货API")
                data = await self._make_request("/api/v3/exchangeInfo", use_fapi=False)
            
            if not data or 'symbols' not in data:
                logger.error("无法获取交易对信息")
                return []
            
            symbols = []
            for symbol_info in data.get('symbols', []):
                symbol = symbol_info['symbol']
                if symbol.endswith('USDT') and symbol_info['status'] == 'TRADING':
                    # 对于期货API，检查合约类型
                    if 'contractType' in symbol_info:
                        if symbol_info['contractType'] == 'PERPETUAL':
                            symbols.append(symbol)
                    else:
                        # 现货API，只添加常见的期货交易对
                        if any(base in symbol for base in ['BTC', 'ETH', 'BNB', 'ADA', 'DOT', 'LINK']):
                            symbols.append(symbol)
                
            logger.info(f"找到 {len(symbols)} 个交易对")
            return symbols[:30]  # 限制数量
            
        except Exception as e:
            logger.error(f"获取交易对失败: {e}")
            return []

    async def get_premium_index(self, symbol: str) -> Dict:
        """获取标记价格和基差数据"""
        return await self._make_request("/fapi/v1/premiumIndex", {'symbol': symbol})

    async def get_funding_rate(self, symbol: str) -> Dict:
        """获取资金费率"""
        data = await self._make_request("/fapi/v1/fundingRate", {'symbol': symbol, 'limit': 1})
        return data[0] if data and isinstance(data, list) else {}

    async def get_open_interest(self, symbol: str) -> Dict:
        """获取持仓量数据"""
        return await self._make_request("/fapi/v1/openInterest", {'symbol': symbol})

    async def get_long_short_ratio(self, symbol: str, period: str = '5m') -> Dict:
        """获取多空比数据"""
        data = await self._make_request("/futures/data/globalLongShortAccountRatio", 
                                      {'symbol': symbol, 'period': period, 'limit': 1})
        return data[0] if data and isinstance(data, list) else {}

    async def get_top_trader_ratio(self, symbol: str, period: str = '5m') -> Dict:
        """获取大户多空比数据"""
        try:
            account_data = await self._make_request("/futures/data/topLongShortAccountRatio",
                                                  {'symbol': symbol, 'period': period, 'limit': 1})
            position_data = await self._make_request("/futures/data/topLongShortPositionRatio",
                                                   {'symbol': symbol, 'period': period, 'limit': 1})
            
            return {
                'account': account_data[0] if account_data and isinstance(account_data, list) else {},
                'position': position_data[0] if position_data and isinstance(position_data, list) else {}
            }
            
        except Exception as e:
            logger.error(f"获取大户数据失败 {symbol}: {e}")
            return {'account': {}, 'position': {}}

    async def get_taker_ratio(self, symbol: str, period: str = '5m') -> Dict:
        """获取主动买卖比数据"""
        data = await self._make_request("/futures/data/takerlongshortRatio",
                                      {'symbol': symbol, 'period': period, 'limit': 1})
        return data[0] if data and isinstance(data, list) else {}

    async def get_contract_data(self, symbol: str) -> Optional[ContractData]:
        """获取完整的合约数据"""
        try:
            # 依次获取数据，即使某些失败也继续
            premium_data = await self.get_premium_index(symbol)
            await asyncio.sleep(0.1)  # 避免过快请求
            
            funding_data = await self.get_funding_rate(symbol)
            await asyncio.sleep(0.1)
            
            oi_data = await self.get_open_interest(symbol)
            await asyncio.sleep(0.1)
            
            ls_data = await self.get_long_short_ratio(symbol)
            await asyncio.sleep(0.1)
            
            top_trader_data = await self.get_top_trader_ratio(symbol)
            await asyncio.sleep(0.1)
            
            taker_data = await self.get_taker_ratio(symbol)
            
            # 提取数据，即使部分API失败也能工作
            mark_price = float(premium_data.get('markPrice', 0)) if premium_data else 0
            index_price = float(premium_data.get('indexPrice', 0)) if premium_data else 0
            
            basis = mark_price - index_price if mark_price and index_price else 0
            basis_percent = (basis / index_price * 100) if index_price else 0
            
            last_funding_rate = float(funding_data.get('fundingRate', 0)) if funding_data else 0
            oi = float(oi_data.get('openInterest', 0)) if oi_data else 0
            long_short_ratio = float(ls_data.get('longShortRatio', 0)) if ls_data else 0
            
            top_account_ratio = 0
            top_position_ratio = 0
            if top_trader_data:
                top_account_ratio = float(top_trader_data.get('account', {}).get('longShortRatio', 0))
                top_position_ratio = float(top_trader_data.get('position', {}).get('longShortRatio', 0))
            
            taker_ratio = float(taker_data.get('buySellRatio', 0)) if taker_data else 0
            
            # 如果关键数据都没有，跳过这个交易对
            if not any([mark_price, last_funding_rate, oi]):
                logger.warning(f"跳过 {symbol}：缺少关键数据")
                return None
            
            return ContractData(
                symbol=symbol,
                mark_price=mark_price,
                index_price=index_price,
                basis=basis,
                basis_percent=basis_percent,
                last_funding_rate=last_funding_rate,
                oi=oi,
                long_short_account_ratio=long_short_ratio,
                top_trader_account_ls_ratio=top_account_ratio,
                top_trader_position_ls_ratio=top_position_ratio,
                taker_buy_sell_ratio=taker_ratio,
                timestamp=datetime.now().isoformat()
            )
            
        except Exception as e:
            logger.error(f"获取合约数据失败 {symbol}: {e}")
            return None

class DataStorage:
    """数据存储管理器"""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    def save_data(self, contract_data: ContractData):
        """保存数据到CSV文件"""
        filename = os.path.join(self.data_dir, f"{contract_data.symbol}.csv")
        
        data_dict = {
            'timestamp': contract_data.timestamp,
            'mark_price': contract_data.mark_price,
            'index_price': contract_data.index_price,
            'basis': contract_data.basis,
            'basis_percent': contract_data.basis_percent,
            'last_funding_rate': contract_data.last_funding_rate,
            'oi': contract_data.oi,
            'long_short_account_ratio': contract_data.long_short_account_ratio,
            'top_trader_account_ls_ratio': contract_data.top_trader_account_ls_ratio,
            'top_trader_position_ls_ratio': contract_data.top_trader_position_ls_ratio,
            'taker_buy_sell_ratio': contract_data.taker_buy_sell_ratio
        }
        
        df = pd.DataFrame([data_dict])
        
        try:
            df.to_csv(filename, index=False)
            logger.debug(f"数据已保存: {contract_data.symbol}")
        except Exception as e:
            logger.error(f"保存数据失败 {contract_data.symbol}: {e}")

    def get_summary_data(self) -> pd.DataFrame:
        """获取汇总数据"""
        summary_data = []
        
        if not os.path.exists(self.data_dir):
            return pd.DataFrame()
            
        for file in os.listdir(self.data_dir):
            if file.endswith('.csv'):
                try:
                    df = pd.read_csv(os.path.join(self.data_dir, file))
                    if not df.empty:
                        summary_data.append(df.iloc[0])
                except Exception as e:
                    logger.error(f"读取文件失败 {file}: {e}")
        
        if summary_data:
            return pd.DataFrame(summary_data)
        return pd.DataFrame()

class AlertSystem:
    """告警系统"""

    def __init__(self, telegram_bot_token: str = None, telegram_chat_id: str = None):
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id
        self.funding_rate_threshold = 0.001  # 0.1%
        self.basis_threshold = 0.5  # 基差阈值

    def check_whale_exit_signal(self, contract_data: ContractData) -> bool:
        """检测庄家退出信号"""
        try:
            extreme_funding = abs(contract_data.last_funding_rate) > self.funding_rate_threshold
            high_basis = abs(contract_data.basis_percent) > self.basis_threshold
            unusual_ls_ratio = (contract_data.long_short_account_ratio > 2.0 or 
                               contract_data.long_short_account_ratio < 0.5)
            
            signal_detected = extreme_funding and (high_basis or unusual_ls_ratio)
            
            if signal_detected:
                logger.warning(f"检测到庄家退出信号: {contract_data.symbol}")
                logger.info(f"  资金费率: {contract_data.last_funding_rate:.4%}")
                logger.info(f"  基差: {contract_data.basis_percent:.2f}%")
                logger.info(f"  多空比: {contract_data.long_short_account_ratio:.2f}")
            
            return signal_detected
            
        except Exception as e:
            logger.error(f"检测庄家信号失败 {contract_data.symbol}: {e}")
            return False

    def send_telegram_alert(self, message: str):
        """发送Telegram告警"""
        if not self.telegram_bot_token or not self.telegram_chat_id:
            logger.info("Telegram配置未设置，跳过通知")
            return
        
        try:
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            data = {
                'chat_id': self.telegram_chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }
            
            response = requests.post(url, data=data, timeout=10)
            
            if response.status_code == 200:
                logger.info("Telegram通知发送成功")
            else:
                logger.error(f"Telegram通知发送失败: {response.text}")
                
        except Exception as e:
            logger.error(f"发送Telegram通知出错: {e}")

    def format_alert_message(self, contract_data: ContractData) -> str:
        """格式化告警消息"""
        return f"""🚨 <b>庄家退出信号</b> 🚨

💰 交易对: <code>{contract_data.symbol}</code>
📊 标记价格: ${contract_data.mark_price:.4f}
💸 资金费率: {contract_data.last_funding_rate:.4%}
📈 基差: {contract_data.basis_percent:.2f}%
⚖️ 多空比: {contract_data.long_short_account_ratio:.2f}

🕐 时间: {contract_data.timestamp[:19]}
🤖 GitHub Actions 监控"""

class CryptoMonitor:
    """加密货币监控主程序"""

    def __init__(self, telegram_bot_token: str = None, telegram_chat_id: str = None):
        self.storage = DataStorage()
        self.alert_system = AlertSystem(telegram_bot_token, telegram_chat_id)
        self.max_concurrent_requests = 1  # 降低并发数避免限流
    
    async def run_scan(self):
        """执行单次扫描"""
        logger.info("开始GitHub Actions扫描...")
        start_time = time.time()
        
        try:
            async with BinanceDataFetcher() as fetcher:
                symbols = await fetcher.get_all_usdt_symbols()
                
                if not symbols:
                    logger.error("未找到交易对，可能API访问受限")
                    return
                
                logger.info(f"开始处理 {len(symbols)} 个交易对...")
                
                processed_count = 0
                alert_count = 0
                
                # 顺序处理以避免限流
                for symbol in symbols:
                    try:
                        contract_data = await fetcher.get_contract_data(symbol)
                        
                        if contract_data:
                            self.storage.save_data(contract_data)
                            processed_count += 1
                            
                            if self.alert_system.check_whale_exit_signal(contract_data):
                                alert_msg = self.alert_system.format_alert_message(contract_data)
                                print("\n" + "=" * 50)
                                print("检测到庄家退出信号!")
                                print(f"交易对: {contract_data.symbol}")
                                print(f"资金费率: {contract_data.last_funding_rate:.4%}")
                                print(f"基差: {contract_data.basis_percent:.2f}%")
                                print("=" * 50 + "\n")
                                
                                self.alert_system.send_telegram_alert(alert_msg)
                                alert_count += 1
                            
                            # 每处理10个显示进度
                            if processed_count % 10 == 0:
                                logger.info(f"已处理: {processed_count}/{len(symbols)}")
                        
                        # 延迟避免限流
                        await asyncio.sleep(0.5)
                        
                    except Exception as e:
                        logger.error(f"处理 {symbol} 时出错: {e}")
                        continue
            
            elapsed_time = time.time() - start_time
            logger.info(f"扫描完成: {processed_count}/{len(symbols)} 个交易对")
            logger.info(f"触发警报: {alert_count}")
            logger.info(f"耗时: {elapsed_time:.2f} 秒")
            
            self.print_summary()
            
        except Exception as e:
            logger.error(f"扫描过程出错: {e}")
            raise

    def print_summary(self):
        """打印汇总信息"""
        try:
            summary_df = self.storage.get_summary_data()
            if not summary_df.empty:
                logger.info(f"本次扫描汇总:")
                logger.info(f"总交易对数: {len(summary_df)}")
                
                # 显示异常资金费率的交易对
                if 'last_funding_rate' in summary_df.columns:
                    extreme_funding = summary_df[abs(summary_df['last_funding_rate']) > 0.001]
                    if not extreme_funding.empty:
                        logger.info(f"极端资金费率交易对 ({len(extreme_funding)}个):")
                        for _, row in extreme_funding.head(5).iterrows():
                            logger.info(f"  {row.get('symbol', 'N/A')}: {row.get('last_funding_rate', 0):.4%}")
                
                # 显示异常基差的交易对
                if 'basis_percent' in summary_df.columns:
                    extreme_basis = summary_df[abs(summary_df['basis_percent']) > 0.5]
                    if not extreme_basis.empty:
                        logger.info(f"极端基差交易对 ({len(extreme_basis)}个):")
                        for _, row in extreme_basis.head(5).iterrows():
                            logger.info(f"  {row.get('symbol', 'N/A')}: {row.get('basis_percent', 0):.2f}%")
            else:
                logger.info("没有生成汇总数据")
                        
        except Exception as e:
            logger.error(f"生成汇总失败: {e}")

def main():
    """主函数"""
    logger.info("GitHub Actions 加密货币监控启动")
    
    # 从环境变量获取Telegram配置
    telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
    
    if telegram_bot_token and telegram_chat_id:
        logger.info("Telegram通知已配置")
    else:
        logger.info("未配置Telegram通知")
    
    monitor = CryptoMonitor(
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id
    )
    
    try:
        asyncio.run(monitor.run_scan())
        logger.info("监控任务完成")
        
    except Exception as e:
        logger.error(f"监控任务失败: {e}")
        # 不再抛出异常，让GitHub Actions继续运行
        return

if __name__ == "__main__":
    main()
