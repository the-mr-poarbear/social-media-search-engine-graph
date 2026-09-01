import argparse
import os
import shutil
import subprocess
import sys
import time

def is_windows_terminal_available() -> bool:
    """Checks if Windows Terminal (wt.exe) is installed and in PATH."""
    return shutil.which("wt.exe") is not None or shutil.which("wt") is not None

def launch_tab(title: str, command: str, cwd: str, is_first: bool = False, use_wt: bool = True):
    abs_cwd = os.path.abspath(cwd)
    
    if use_wt:
        # Windows Terminal: '-w 0' attaches the new tab to the existing window
        # '-d' sets directory, '--title' sets tab name
        args = ["wt.exe"]
        if not is_first:
            args.extend(["-w", "0", "new-tab"])
        
        args.extend([
            "--title", title,
            "-d", abs_cwd,
            "cmd.exe", "/k", command
        ])
        
        subprocess.Popen(args)
    else:
        # Fallback to standalone cmd.exe window if wt.exe is not available
        cmd = f'title {title} && {command}'
        subprocess.Popen(
            ["cmd.exe", "/k", cmd],
            creationflags=subprocess.CREATE_NEW_CONSOLE,
            cwd=abs_cwd
        )

def main():
    parser = argparse.ArgumentParser(description="Social Web Pipeline Runner with Tab Support")
    parser.add_argument(
        "-n", "--consumers", 
        type=int, 
        default=8, 
        help="Number of graph_consumer workers to spawn (default: 8)"
    )
    parser.add_argument(
        "--wait", 
        type=int, 
        default=5, 
        help="Seconds to wait after starting singbox proxies before spawning workers (default: 5)"
    )
    parser.add_argument(
        "--cwd", 
        type=str, 
        default=".", 
        help="Working directory to execute modules from (default: current directory)"
    )
    
    args = parser.parse_args()
    has_wt = is_windows_terminal_available()
    
    mode_str = "Tabs in 1 Windows Terminal" if has_wt else "Separate Windows (Fallback)"
    print("=" * 65)
    print(f"🚀 [Mode: {mode_str}]")
    print(f"📦 Step 1: Starting Singbox Proxy Pool in Tab 1...")
    print("=" * 65)
    
    # 1. Launch Singbox proxies in the first tab
    launch_tab(
        title="Singbox Proxies",
        command="python -m scripts.run_singbox_proxies",
        cwd=args.cwd,
        is_first=True,
        use_wt=has_wt
    )
    
    print(f"⏳ Waiting {args.wait}s for proxies to bind ports and pass healthchecks...")
    time.sleep(args.wait)
    
    print("\n" + "=" * 65)
    print(f"⚡ Step 2: Adding {args.consumers} Consumer Worker Tabs to the same window...")
    print("=" * 65)
    
    # 2. Add N Graph Consumer tabs to the same window
    for i in range(1, args.consumers + 1):
        print(f" -> Opening Tab: Graph Consumer #{i}...")
        launch_tab(
            title=f"Consumer #{i}",
            command="py -m app.graph_consumer",
            cwd=args.cwd,
            is_first=False,
            use_wt=has_wt
        )
        time.sleep(0.35)  # slight delay to let Windows Terminal register the tab

    print("\n" + "=" * 65)
    print(f"✅ All {args.consumers} workers + Singbox are open as tabs in one window!")
    print("=" * 65)

if __name__ == "__main__":
    main()
