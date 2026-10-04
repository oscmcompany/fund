"""Print the production Rust lines a branch adds against a base ref, by file, with the review budget."""

import re
import subprocess
import sys

base = sys.argv[1] if len(sys.argv) > 1 else "origin/master"
diff = subprocess.run(
    ["git", "diff", "--merge-base", base, "HEAD", "--unified=0", "--", "*.rs"],
    capture_output=True, text=True, check=True,
).stdout

added = {}
path = None
for line in diff.splitlines():
    if line.startswith("+++ "):
        name = line[6:] if line.startswith("+++ b/") else None
        path = name if name and not name.startswith(("src_old/", "tests/")) else None
    elif path and line.startswith("@@"):
        start, _, count = re.match(r"@@ -\S+ \+(\d+)(,(\d+))? @@", line).groups()
        lines = range(int(start), int(start) + int(count if count is not None else 1))
        added.setdefault(path, []).extend(lines)

total = 0
for path, lines in sorted(added.items()):
    source = subprocess.run(["git", "show", f"HEAD:{path}"], capture_output=True, text=True, check=True).stdout
    # Everything from the first test module down is test code in this repository's layout.
    test_start = next((index for index, text in enumerate(source.splitlines(), 1) if text.strip() == "#[cfg(test)]"), None)
    production = [number for number in lines if test_start is None or number < test_start]
    if not production:
        continue
    total += len(production)
    print(f"{path}: {len(production)} lines, first {production[0]}, last {production[-1]}")

print(f"production lines added: {total}")
print(f"review budget: {min(250, total // 4)}")
