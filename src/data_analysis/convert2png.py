import os

from svglib.svglib import svg2rlg
from reportlab.graphics import renderPM


in_path = "vizs/timelapse/svg"
out_path = "vizs/timelapse/png"

svgs = [
    os.path.join(in_path, f)
    for f in os.listdir(in_path)
    if os.path.isfile(os.path.join(in_path, f))
]
for svg in svgs:
    fname = svg.split(".")[0] + ".png"
    fname = fname.replace("svg", "png")
    drawing = svg2rlg(svg)
    renderPM.drawToFile(drawing, fname, fmt="PNG")
    print(f"{fname} is ready")
