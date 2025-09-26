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
        # GitHub Actions 运行在境外，可以直接访问
        self.base_urls = [
            "https://fapi.binance.com",
            "https://dapi.binance.com",
        ]
        
        self.current_base_url = self.base_urls[0]
        self.session = None
        self.request_timeout = 15  # 降低超时时间
        self.max_retries = 2  # 减少重试次数

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=self.request_timeout)
        
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers={'User-Agent': 'CryptoMonitor/1.0'}
        )
        
        await self._test_connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _test_connection(self):
        """测试API连接"""
        for base_url in self.base_urls:
            try:
                test_url = f"{base_url}/fapi/v1/ping"
                async with self.session.get(test_url, timeout=10) as response:
                    if response.status == 200:
                        self.current_base_url = base_url
                        logger.info(f"Successfully connected to {base_url}")
                        return
            except Exception as e:
                logger.warning(f"Failed to connect to {base_url}: {e}")
                continue
        
        logger.error("无法连接到Binance API")
        raise Exception("API连接失败")

    async def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """统一的请求方法"""
        for attempt in range(self.max_retries):
            try:
                url = f"{self.current_base_url}{endpoint}"
                
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:
                        wait_time = 1 + attempt
                        logger.warning(f"API限流，等待{wait_time}秒...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"API请求失败: {response.status}")
                        
            except asyncio.TimeoutError:
                logger.warning(f"请求超时 (尝试 {attempt + 1}/{self.max_retries})")
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"请求异常: {e}")
                await asyncio.sleep(0.5)
        
        return {}

    async def get_all_usdt_symbols(self) -> List[str]:
        """获取所有USDT永续合约交易对"""
        try:
            data = await self._make_request("/fapi/v1/exchangeInfo")
            
            if not data:
                return []
            
            symbols = []
            for symbol_info in data.get('symbols', []):
                if (symbol_info['symbol'].endswith('USDT') and 
                    symbol_info['status'] == 'TRADING' and
                    symbol_info['contractType'] == 'PERPETUAL'):
                    symbols.append(symbol_info['symbol'])
                
            logger.info(f"找到 {len(symbols)} 个USDT永续合约")
            return symbols[:50]  # 限制数量以节省运行时间
            
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
            tasks = [
                self.get_premium_index(symbol),
                self.get_funding_rate(symbol),
                self.get_open_interest(symbol),
                self.get_long_short_ratio(symbol),
                self.get_top_trader_ratio(symbol),
                self.get_taker_ratio(symbol)
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            premium_data, funding_data, oi_data, ls_data, top_trader_data, taker_data = results
            
            failed_count = sum(1 for result in results if isinstance(result, Exception))
            if failed_count > 3:
                logger.warning(f"太多请求失败，跳过 {symbol}")
                return None
            
            mark_price = float(premium_data.get('markPrice', 0)) if isinstance(premium_data, dict) else 0
            index_price = float(premium_data.get('indexPrice', 0)) if isinstance(premium_data, dict) else 0
            
            basis = mark_price - index_price if mark_price and index_price else 0
            basis_percent = (basis / index_price * 100) if index_price else 0
            
            last_funding_rate = float(funding_data.get('fundingRate', 0)) if isinstance(funding_data, dict) else 0
            oi = float(oi_data.get('openInterest', 0)) if isinstance(oi_data, dict) else 0
            long_short_ratio = float(ls_data.get('longShortRatio', 0)) if isinstance(ls_data, dict) else 0
            
            top_account_ratio = 0
            top_position_ratio = 0
            if isinstance(top_trader_data, dict):
                top_account_ratio = float(top_trader_data.get('account', {}).get('longShortRatio', 0))
                top_position_ratio = float(top_trader_data.get('position', {}).get('longShortRatio', 0))
            
            taker_ratio = float(taker_data.get('buySellRatio', 0)) if isinstance(taker_data, dict) else 0
            
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
    """数据存储管理器 - 适配GitHub Actions"""

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
            df.to_csv(filename, index=False)  # 每次覆盖写入，因为Actions是无状态的
            logger.debug(f"数据已保存: {contract_data.symbol}")
        except Exception as e:
            logger.error(f"保存数据失败 {contract_data.symbol}: {e}")

    def get_summary_data(self) -> pd.DataFrame:
        """获取汇总数据"""
        summary_data = []
        
        for file in os.listdir(self.data_dir):
            if file.endswith('.csv'):
                try:
                    df = pd.read_csv(os.path.join(self.data_dir, file))
                    if not df.empty:
                        summary_data.append(df.iloc[0])  # 获取最新记录
                except Exception as e:
                    logger.error(f"读取文件失败 {file}: {e}")
        
        if summary_data:
            return pd.DataFrame(summary_data)
        return pd.DataFrame()

class AlertSystem:
    """告警系统 - GitHub Actions版本"""

    def __init__(self, telegram_bot_token: str = None, telegram_chat_id: str = None):
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id
        self.funding_rate_threshold = 0.001  # 0.1%
        self.basis_threshold = 0.5  # 基差阈值

    def check_whale_exit_signal(self, contract_data: ContractData) -> bool:
        """检测庄家退出信号"""
        try:
            # 简化的信号检测逻辑
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
    """加密货币监控主程序 - GitHub Actions版本"""

    def __init__(self, telegram_bot_token: str = None, telegram_chat_id: str = None):
        self.storage = DataStorage()
        self.alert_system = AlertSystem(telegram_bot_token, telegram_chat_id)
        self.max_concurrent_requests = 2  # 降低并发数
    
    async def run_scan(self):
        """执行单次扫描"""
        logger.info("🚀 开始GitHub Actions扫描...")
        start_time = time.time()
        
        async with BinanceDataFetcher() as fetcher:
            symbols = await fetcher.get_all_usdt_symbols()
            
            if not symbols:
                logger.error("未找到交易对")
                return
            
            semaphore = asyncio.Semaphore(self.max_concurrent_requests)
            processed_count = 0
            alert_count = 0
            
            async def process_symbol(symbol):
                nonlocal processed_count, alert_count
                
                async with semaphore:
                    try:
                        contract_data = await fetcher.get_contract_data(symbol)
                        
                        if contract_data:
                            self.storage.save_data(contract_data)
                            processed_count += 1
                            
                            if self.alert_system.check_whale_exit_signal(contract_data):
                                alert_msg = self.alert_system.format_alert_message(contract_data)
                                print("\n" + "=" * 50)
                                print("🚨 检测到庄家退出信号!")
                                print(f"交易对: {contract_data.symbol}")
                                print(f"资金费率: {contract_data.last_funding_rate:.4%}")
                                print(f"基差: {contract_data.basis_percent:.2f}%")
                                print("=" * 50 + "\n")
                                
                                self.alert_system.send_telegram_alert(alert_msg)
                                alert_count += 1
                            
                            await asyncio.sleep(0.1)
                            
                    except Exception as e:
                        logger.error(f"处理 {symbol} 时出错: {e}")
            
            await asyncio.gather(*[process_symbol(symbol) for symbol in symbols], return_exceptions=True)
        
        elapsed_time = time.time() - start_time
        logger.info(f"✅ 扫描完成: {processed_count}/{len(symbols)} 个交易对")
        logger.info(f"🚨 触发警报: {alert_count}")
        logger.info(f"⏱️ 耗时: {elapsed_time:.2f} 秒")
        
        # 输出汇总信息
        self.print_summary()

    def print_summary(self):
        """打印汇总信息"""
        try:
            summary_df = self.storage.get_summary_data()
            if not summary_df.empty:
                logger.info(f"📊 本次扫描汇总:")
                
                # 显示异常资金费率的交易对
                extreme_funding = summary_df[abs(summary_df['last_funding_rate']) > 0.001]
                if not extreme_funding.empty:
                    logger.info(f"💸 极端资金费率 (>{0.1:.1%}):")
                    for _, row in extreme_funding.head(5).iterrows():
                        logger.info(f"  {row['symbol']}: {row['last_funding_rate']:.4%}")
                
                # 显示异常基差的交易对
                extreme_basis = summary_df[abs(summary_df['basis_percent']) > 0.5]
                if not extreme_basis.empty:
                    logger.info(f"📈 极端基差 (>{0.5:.1%}):")
                    for _, row in extreme_basis.head(5).iterrows():
                        logger.info(f"  {row['symbol']}: {row['basis_percent']:.2f}%")
                        
        except Exception as e:
            logger.error(f"生成汇总失败: {e}")

def main():
    """主函数 - 适配GitHub Actions"""
    logger.info("🤖 GitHub Actions 加密货币监控启动")
    
    # 从环境变量获取Telegram配置
    telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
    
    if telegram_bot_token and telegram_chat_id:
        logger.info("✅ Telegram通知已配置")
    else:
        logger.info("ℹ️ 未配置Telegram通知")
    
    monitor = CryptoMonitor(
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id
    )
    
    try:
        asyncio.run(monitor.run_scan())
        logger.info("🎉 监控任务完成")
        
    except Exception as e:
        logger.error(f"💥 监控任务失败: {e}")
        raise

if __name__ == "__main__":
    main()
