import numpy as np
from PIL import Image

im1 = np.arange(2560 * 2160, dtype=np.uint16).reshape(2160, 2560) // 100
im2 = np.arange(2560 * 2160, dtype=np.uint16).reshape(2560, 2160).T // 100
Image.fromarray(im1).save(
    "myplate_B03_T0002F001L01A01Z01C01.png", optimize=True
)
Image.fromarray(im2).save(
    "myplate_B03_T0002F001L01A01Z02C01.png", optimize=True
)
Image.fromarray(im2).save(
    "myplate_B03_T0002F002L01A01Z01C01.png", optimize=True
)
Image.fromarray(im1).save(
    "myplate_B03_T0002F002L01A01Z02C01.png", optimize=True
)
