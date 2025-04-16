#!/usr/bin/env python3
import sys
import re

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue  # Bỏ qua dòng trống

    try:
        year, temp, q = line.split("\t")  # dùng tab
        if temp != "9999" and re.match(r"^[01459]$", q):
            print(f"{year}\t{temp}")
    except Exception as e:
        continue  # Bỏ qua dòng lỗi
