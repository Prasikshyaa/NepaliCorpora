"""
Real-time crawl progress monitoring script.

Usage:
    python scripts/check_crawl_progress.py
    
    # Or run continuously
    watch -n 30 python scripts/check_crawl_progress.py
"""

import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any

from scripts.utils import paths


def get_site_stats(db_path: Path) -> Dict[str, Any]:
    """Get statistics for a single site from its SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        
        # Basic stats
        stats = cur.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN visited=1 THEN 1 ELSE 0 END) as visited,
                SUM(CASE WHEN is_article=1 THEN 1 ELSE 0 END) as articles,
                MAX(depth) as max_depth_reached
            FROM urls
        """).fetchone()
        
        total, visited, articles, max_depth = stats
        pending = total - visited
        progress = (visited / total * 100) if total > 0 else 0
        
        # Get last activity timestamp
        last_activity = cur.execute("""
            SELECT MAX(discovered_at) FROM urls WHERE visited=1
        """).fetchone()[0]
        
        # Depth distribution
        depth_dist = cur.execute("""
            SELECT depth, COUNT(*) 
            FROM urls 
            GROUP BY depth 
            ORDER BY depth
        """).fetchall()
        
        conn.close()
        
        return {
            "total": total,
            "visited": visited,
            "pending": pending,
            "articles": articles,
            "max_depth": max_depth or 0,
            "progress": progress,
            "last_activity": last_activity,
            "depth_distribution": depth_dist,
        }
    except Exception as e:
        return {"error": str(e)}


def format_timedelta(td: timedelta) -> str:
    """Format timedelta as human-readable string."""
    total_seconds = int(td.total_seconds())
    
    if total_seconds < 60:
        return f"{total_seconds}s ago"
    elif total_seconds < 3600:
        return f"{total_seconds // 60}m ago"
    elif total_seconds < 86400:
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        return f"{hours}h {minutes}m ago"
    else:
        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        return f"{days}d {hours}h ago"


def print_progress_bar(progress: float, width: int = 30) -> str:
    """Generate a text progress bar."""
    filled = int(width * progress / 100)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {progress:5.1f}%"


def main():
    """Main progress monitoring function."""
    crawl_state_dir = paths.DATA_RAW / "crawl_state"
    
    if not crawl_state_dir.exists():
        print("❌ No crawl state directory found")
        print(f"   Expected: {crawl_state_dir}")
        return
    
    db_files = sorted(crawl_state_dir.glob("*.sqlite"))
    
    if not db_files:
        print("❌ No crawl databases found")
        print(f"   Directory: {crawl_state_dir}")
        return
    
    print("="*100)
    print(f"{'CRAWL PROGRESS REPORT':^100}")
    print(f"{'Generated at: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'):^100}")
    print("="*100)
    
    total_articles = 0
    total_pending = 0
    active_crawls = 0
    completed_crawls = 0
    
    for db_path in db_files:
        site_name = db_path.stem
        stats = get_site_stats(db_path)
        
        if "error" in stats:
            print(f"\n❌ {site_name:20} | ERROR: {stats['error']}")
            continue
        
        # Determine status
        if stats["pending"] == 0 and stats["visited"] > 0:
            status = "✅ COMPLETE"
            completed_crawls += 1
        elif stats["last_activity"]:
            try:
                last_time = datetime.fromisoformat(stats["last_activity"])
                time_since = datetime.now() - last_time
                
                if time_since < timedelta(minutes=10):
                    status = f"🔄 ACTIVE ({format_timedelta(time_since)})"
                    active_crawls += 1
                else:
                    status = f"⏸️  STALLED ({format_timedelta(time_since)})"
            except:
                status = "⚠️  UNKNOWN"
        else:
            status = "⏳ QUEUED"
        
        print(f"\n{'─'*100}")
        print(f"📰 {site_name.upper():30} | {status}")
        print(f"{'─'*100}")
        
        # Progress bar
        print(f"   Progress:  {print_progress_bar(stats['progress'])}")
        
        # Statistics
        print(f"   Visited:   {stats['visited']:>8,} URLs")
        print(f"   Pending:   {stats['pending']:>8,} URLs")
        print(f"   Articles:  {stats['articles']:>8,} found")
        print(f"   Max Depth: {stats['max_depth']:>8}")
        
        # Depth distribution (compact)
        if stats["depth_distribution"]:
            depth_str = " | ".join(
                f"D{d}:{c:,}" 
                for d, c in stats["depth_distribution"][:8]  # Show first 8 depths
            )
            print(f"   Depths:    {depth_str}")
        
        total_articles += stats["articles"]
        total_pending += stats["pending"]
    
    # Summary
    print("\n" + "="*100)
    print(f"{'SUMMARY':^100}")
    print("="*100)
    print(f"  Sites crawling:  {active_crawls}")
    print(f"  Sites completed: {completed_crawls}")
    print(f"  Sites pending:   {len(db_files) - active_crawls - completed_crawls}")
    print(f"  Total articles:  {total_articles:,}")
    print(f"  Total pending:   {total_pending:,}")
    print("="*100)
    
    # Recommendations
    if total_pending == 0 and completed_crawls == len(db_files):
        print("\n✅ All crawls completed! Ready for article scraping.")
    elif active_crawls > 0:
        print(f"\n🔄 {active_crawls} crawl(s) in progress. Check back later.")
    else:
        print("\n⚠️  No active crawls detected. Consider restarting the crawler.")


if __name__ == "__main__":
    main()