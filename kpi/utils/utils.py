import pandas as pd


def ranged_values_to_colors_3p(
    row, key: str, b_ceil: int, g_origin: int, r_ceil: int
) -> tuple[float, float, float]:
    """Convert a value into a RGB tuple given a configuration"""
    b = 0.0
    g = 0.0
    r = 0.0
    if not pd.isnull(row[key]):
        if row[key] <= b_ceil:
            b = 1.0
        elif row[key] < g_origin:
            b = abs(row[key] - g_origin) / abs(b_ceil - g_origin)
            g = abs(row[key] - b_ceil) / abs(b_ceil - g_origin)
        elif row[key] < r_ceil:
            r = abs(row[key] - g_origin) / abs(r_ceil - g_origin)
            g = abs(row[key] - r_ceil) / abs(r_ceil - g_origin)
        else:
            r = 1.0

    return (r, g, b)
