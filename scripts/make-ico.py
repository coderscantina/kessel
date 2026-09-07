#!/usr/bin/env python3
"""Pack PNGs into a Windows .ico.

    make-ico.py out.ico 16.png 32.png 48.png …

Every entry is stored as a PNG blob, which Windows has read since Vista and
which keeps the alpha channel intact — no BMP/AND-mask conversion needed. The
file is an ICONDIR header, one 16-byte ICONDIRENTRY per image, then the blobs.
Called by scripts/gen-icons.sh; sizes are read back out of each PNG's IHDR so
the index can't drift from the artwork.
"""

import struct
import sys


def png_size(blob: bytes) -> tuple[int, int]:
    """Width and height from a PNG's IHDR, which is always the first chunk."""
    if blob[:8] != b"\x89PNG\r\n\x1a\n" or blob[12:16] != b"IHDR":
        raise SystemExit("not a PNG (or a PNG without a leading IHDR)")
    return struct.unpack(">II", blob[16:24])


def main() -> None:
    if len(sys.argv) < 3:
        raise SystemExit(f"usage: {sys.argv[0]} out.ico image.png [image.png …]")
    out, sources = sys.argv[1], sys.argv[2:]

    images = []
    for path in sources:
        with open(path, "rb") as handle:
            blob = handle.read()
        width, height = png_size(blob)
        if not 1 <= width <= 256 or not 1 <= height <= 256:
            raise SystemExit(f"{path}: {width}x{height} is out of range for an .ico")
        images.append((width, height, blob))
    images.sort(key=lambda image: image[0])

    header = struct.pack("<HHH", 0, 1, len(images))  # reserved, type=icon, count
    offset = len(header) + 16 * len(images)
    entries, blobs = bytearray(), bytearray()
    for width, height, blob in images:
        entries += struct.pack(
            "<BBBBHHII",
            width % 256,  # 256 is stored as 0
            height % 256,
            0,  # palette size: 0 for truecolor
            0,  # reserved
            1,  # color planes
            32,  # bits per pixel
            len(blob),
            offset,
        )
        blobs += blob
        offset += len(blob)

    with open(out, "wb") as handle:
        handle.write(header + entries + blobs)


if __name__ == "__main__":
    main()
