#!/usr/bin/env python3
# collector.py – fetch Marine / Surf artefacts into timestamped bundle
from __future__ import annotations
import asyncio, json, logging, sys, time, uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Optional

import aiohttp, configparser
import utils

# Import the organized agent modules
import agents
import asyncio
import importlib

log = utils.log_init("collector")

# --------------------------------------------------------------------- #
class Ctx:
    """Context class for data collection"""
    def __init__(self, cfg: configparser.ConfigParser):
        self.cfg = cfg
        self.run_id = f"{uuid.uuid4().hex}_{int(time.time())}"
        self.base = Path(cfg["GENERAL"]["data_dir"]).expanduser()
        self.bundle = self.base / self.run_id
        self.bundle.mkdir(parents=True, exist_ok=True)
        self.headers = {"User-Agent": cfg["GENERAL"]["user_agent"]}
        self.timeout = int(cfg["GENERAL"]["timeout"])
        self.retries = int(cfg["GENERAL"]["max_retries"])
        self.throttle = int(cfg["GENERAL"]["windy_throttle_seconds"])
        self.last_call: Dict[str, float] = {}

        # Create SSL context for non-verified connections
        import ssl
        self.no_verify_ssl = ssl.create_default_context()
        self.no_verify_ssl.check_hostname = False
        self.no_verify_ssl.verify_mode = ssl.CERT_NONE
    
    async def fetch(self, sess: aiohttp.ClientSession, url: str,
                   *, method: str = "GET", json_body=None, **kwargs) -> bytes | None:
        """Extra kwargs are ignored for compatibility with model_agent.py"""
        """Fetch data from a URL with retries and throttling with domain-specific SSL handling"""
        host = url.split("/")[2]
        if "windy.com" in host:
            gap = time.time() - self.last_call.get(host, 0)
            if gap < self.throttle:
                await asyncio.sleep(self.throttle - gap)

        # Get SSL exceptions from config
        ssl_exception_domains = set()
        if "SSL_EXCEPTIONS" in self.cfg and "disable_verification" in self.cfg["SSL_EXCEPTIONS"]:
            exceptions = self.cfg["SSL_EXCEPTIONS"]["disable_verification"].split(',')
            ssl_exception_domains = set(domain.strip() for domain in exceptions if domain.strip())

        for attempt in range(self.retries):
            try:
                # Determine SSL context for this URL
                ssl_ctx = True  # Default: verify SSL

                # Check if this domain should bypass SSL verification
                try:
                    if host and host in ssl_exception_domains:
                        # Use non-verifying SSL context
                        ssl_ctx = self.no_verify_ssl
                        log.debug(f"SSL verification disabled for {host}")
                except Exception as e:
                    log.warning(f"Error determining SSL context: {e}")
                    # Continue with default SSL verification

                # Pass SSL context to request
                if method == "GET":
                    r = await sess.get(url, headers=self.headers, timeout=self.timeout, ssl=ssl_ctx)
                else:
                    r = await sess.request(method, url, headers=self.headers,
                                          timeout=self.timeout, json=json_body, ssl=ssl_ctx)

                if r.status == 200:
                    self.last_call[host] = time.time()
                    return await r.read()
                if r.status == 404:
                    # Downgrade to debug level for 404s - these are common and expected
                    log.debug("HTTP 404 Not Found: %s", url)
                    return None
                if r.status == 403:
                    log.warning("HTTP 403 Forbidden: %s", url)
                    return None
                if r.status == 400 and "windy" in host:
                    # Windy free tier returns 400 for too many params
                    log.debug("HTTP 400 Bad Request (Windy API limit): %s", url)
                    return None
                if r.status == 400 and "stormglass" in host:
                    # Stormglass API limit
                    log.debug("HTTP 400 Bad Request (Stormglass API limit): %s", url)
                    return None
                if r.status in (400, 429, 500):
                    log.warning("HTTP %s %s", r.status, url)
                    back = 2 ** attempt
                    if attempt < self.retries - 1:  # Don't log retry message on last attempt
                        log.debug("Retry in %ss", back)
                    await asyncio.sleep(back)
                else:
                    log.info("HTTP %s %s", r.status, url)
                    return None
            except aiohttp.ClientConnectorCertificateError as e:
                # Handle SSL certificate errors by disabling SSL for this host on retry
                log.warning(f"SSL Certificate error for {url}: {str(e)}")
                # Add to SSL exceptions temporarily for future requests
                ssl_exception_domains.add(host)
                if attempt == self.retries - 1:
                    log.warning(f"SSL Certificate verification failed for {url} after {self.retries} attempts")
                await asyncio.sleep(2 ** attempt)
            except aiohttp.ClientConnectorError as e:
                # Handle connection errors
                log.debug(f"Connection error for {url}: {str(e)}")
                if attempt == self.retries - 1:
                    log.warning(f"Connection failed for {url} after {self.retries} attempts: {str(e)}")
                await asyncio.sleep(2 ** attempt)
            except asyncio.TimeoutError:
                # Handle timeouts
                log.debug(f"Timeout error for {url} – retry in {2 ** attempt}s")
                if attempt == self.retries - 1:
                    log.warning(f"Timeout error for {url} after {self.retries} attempts")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                # Handle other errors
                log.warning(f"Fetch error for {url}: {str(e)} – retry in {2 ** attempt}s")
                await asyncio.sleep(2 ** attempt)
        return None

    def save(self, name: str, data: bytes | str):
        """Save data to the bundle directory (synchronous method for backwards compatibility)"""
        path = self.bundle / name
        path.write_bytes(data if isinstance(data, bytes) else data.encode())
        return path.name

    async def save_async(self, name: str, data: bytes | str):
        """Asynchronously save data to the bundle directory"""
        path = self.bundle / name
        await utils.write_file_async(path, data if isinstance(data, bytes) else data.encode())
        return path.name

# --------------------------------------------------------------------- #
async def collect(cfg, args):
    """Main collection function that orchestrates all data sources"""
    ctx = Ctx(cfg)

    # Prune old bundles using async operations where possible
    cutoff = datetime.now(timezone.utc) - timedelta(days=args.cache_days)

    # First identify directories to clean up
    dirs_to_prune = []
    for d in ctx.base.iterdir():
        if d.is_dir() and datetime.fromtimestamp(
               d.stat().st_mtime, timezone.utc) < cutoff:
            dirs_to_prune.append(d)

    # Clean up old bundles (file deletion is better done in batches rather than one by one)
    for d in dirs_to_prune:
        # Delete files
        delete_tasks = []
        for f in d.iterdir():
            if f.is_file():  # Only unlink files
                try:
                    f.unlink(missing_ok=True)  # This is typically fast enough to be kept synchronous
                except Exception as e:
                    log.warning(f"Failed to delete file {f}: {e}")

        # Remove the directory after files are deleted
        try:
            d.rmdir()
        except Exception as e:
            log.warning(f"Failed to remove directory {d}: {e}")

    enabled = cfg["SOURCES"]
    
    # Use a longer timeout for network operations
    timeout = aiohttp.ClientTimeout(total=120)  # 2 minutes timeout

    # Get domains that should skip SSL verification
    ssl_exception_domains = set()
    if "SSL_EXCEPTIONS" in cfg and "disable_verification" in cfg["SSL_EXCEPTIONS"]:
        exceptions = cfg["SSL_EXCEPTIONS"]["disable_verification"].split(',')
        ssl_exception_domains = set(domain.strip() for domain in exceptions if domain.strip())
        log.info(f"SSL verification disabled for: {', '.join(ssl_exception_domains)}")

    # Create a function that decides whether to verify SSL for each request
    # This allows domain-specific SSL bypass instead of globally disabling SSL
    async def ssl_context_for_url(url, ssl_exception_domains=None):
        """Returns the appropriate SSL context for a given URL"""
        if not ssl_exception_domains:
            return True  # Default: verify SSL

        try:
            # Extract the domain from the URL
            domain = url.split("/")[2]

            # Check if this domain or any of its parent domains should bypass SSL
            current_domain = domain
            while current_domain:
                if current_domain in ssl_exception_domains:
                    log.debug(f"SSL verification disabled for {domain} (matched {current_domain})")
                    return ctx.no_verify_ssl  # Use the SSL context from ctx object

                # Try parent domain by removing leftmost subdomain
                parts = current_domain.split(".", 1)
                if len(parts) > 1:
                    current_domain = parts[1]
                else:
                    break

            return True  # Default: verify SSL

        except Exception as e:
            log.warning(f"Error determining SSL context for {url}: {e}")
            return True  # Default to verification on error

    # Prepare connector with SSL settings that will be determined per request
    connector = aiohttp.TCPConnector(ssl=None)  # Will be set per request
    log.info("Using domain-specific SSL verification bypass")

    session = None
    try:
        session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        
        # Create a list of tasks that can fail independently
        all_tasks = []
        
        # === Chart Agents ===
        if enabled.getboolean("enable_opc"):
            all_tasks.append(asyncio.create_task(agents.opc(ctx, session)))
        if enabled.getboolean("enable_wpc"):
            all_tasks.append(asyncio.create_task(agents.wpc(ctx, session)))
        if enabled.getboolean("enable_nws"):
            all_tasks.append(asyncio.create_task(agents.nws(ctx, session)))

        # === Buoy Agents ===
        if enabled.getboolean("enable_buoys"):
            all_tasks.append(asyncio.create_task(agents.buoys(ctx, session)))
        if enabled.getboolean("enable_coops"):
            all_tasks.append(asyncio.create_task(agents.noaa_coops(ctx, session)))

        # === Model Agents ===
        if enabled.getboolean("enable_pacioos"):
            all_tasks.append(asyncio.create_task(agents.pacioos(ctx, session)))
        if enabled.getboolean("enable_pacioos_swan"):
            all_tasks.append(asyncio.create_task(agents.pacioos_swan(ctx, session)))
        if enabled.getboolean("enable_ecmwf") and cfg["API"].get("ECMWF_KEY", "").strip():
            all_tasks.append(asyncio.create_task(agents.ecmwf_wave(ctx, session)))

        # === API Agents ===
        if enabled.getboolean("enable_windy"):
            all_tasks.append(asyncio.create_task(agents.windy(ctx, session)))
        if enabled.getboolean("enable_open_meteo"):
            all_tasks.append(asyncio.create_task(agents.open_meteo(ctx, session)))
        if enabled.getboolean("enable_stormglass", False) and "STORMGLASS_KEY" in ctx.cfg["API"]:
            all_tasks.append(asyncio.create_task(agents.stormglass(ctx, session)))
        if enabled.getboolean("enable_surfline", True):
            all_tasks.append(asyncio.create_task(agents.surfline(ctx, session)))

        # === Regional Agents ===
        if enabled.getboolean("enable_southern_hemisphere"):
            all_tasks.append(asyncio.create_task(agents.southern_hemisphere(ctx, session)))
        if enabled.getboolean("enable_north_pacific"):
            all_tasks.append(asyncio.create_task(agents.north_pacific_enhanced(ctx, session)))

        # === WW3 model data with improved reliability ===
        if enabled.getboolean("enable_models"):
            try:
                # Try primary WW3 source first (now integrated in agents module)
                model_task = asyncio.create_task(agents.model_agent(ctx, session))

                # Use fallback if primary fails
                model_task.add_done_callback(
                    lambda t: all_tasks.append(asyncio.create_task(agents.ww3_model_fallback(ctx, session)))
                    if t.exception() or not t.result() else None
                )
                all_tasks.append(model_task)

                # Also try the ECMWF agent if enabled
                if enabled.getboolean("enable_ecmwf_alt", False):
                    all_tasks.append(asyncio.create_task(agents.ecmwf_agent(ctx, session)))
            except Exception as e:
                log.error(f"Failed to create model task: {e}")
                # Add fallback directly if model task creation fails
                all_tasks.append(asyncio.create_task(agents.ww3_model_fallback(ctx, session)))

        # Wait for all tasks to complete, handling errors
        results = []
        for task in asyncio.as_completed(all_tasks):
            try:
                result = await task
                if result:  # Only extend if we got actual results
                    results.extend(result)
            except Exception as e:
                log.error(f"Task failed: {str(e)}")

        # Write metadata and update latest bundle pointer using async file operations
        metadata_content = utils.jdump({"run_id": ctx.run_id, "timestamp": utils.utcnow(), "results": results})
        bundle_path = ctx.bundle/"metadata.json"
        latest_bundle_path = ctx.base/"latest_bundle.txt"

        # Use parallel tasks for writing metadata and updating latest bundle
        write_tasks = [
            utils.write_file_async(bundle_path, metadata_content),
            utils.write_file_async(latest_bundle_path, ctx.run_id)
        ]
        await asyncio.gather(*write_tasks)

        log.info("Bundle %s complete (%s files)", ctx.run_id, len(results))
        return ctx.bundle

    except Exception as e:
        log.error(f"Collection process failed: {e}")
        return None

    finally:
        # Ensure all tasks are completed or cancelled before closing session
        for task in asyncio.all_tasks():
            # Skip the main collection task
            if task is asyncio.current_task():
                continue
            try:
                task.cancel()
                # Allow a brief period for tasks to clean up
                await asyncio.sleep(0.1)
            except Exception as e:
                log.warning(f"Error cancelling task: {e}")

        # Ensure session is properly closed
        if session and not session.closed:
            try:
                await session.close()
                # Wait a bit to allow the session to fully close
                await asyncio.sleep(0.25)
            except Exception as e:
                log.error(f"Error closing session: {e}")

# --------------------------------------------------------------------- #
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Collector – grab latest marine artefacts")
    parser.add_argument("--config", default="config.ini",
                       help="INI file to read")
    parser.add_argument("--cache-days", type=int, default=7,
                       help="days to keep bundles")
    args = parser.parse_args()
    
    cfg = configparser.ConfigParser()
    cfg.read(args.config)
    
    try:
        result = asyncio.run(collect(cfg, args))
        if result:
            print(result)
            sys.exit(0)
        else:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nCollection interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

