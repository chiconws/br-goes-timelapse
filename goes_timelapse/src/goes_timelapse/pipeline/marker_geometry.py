from __future__ import annotations


def point_within_polygon(
    point: tuple[float, float],
    polygon: tuple[tuple[float, float], ...],
) -> bool:
    if len(polygon) < 3:
        return False

    vertices = list(polygon)
    if vertices[0] != vertices[-1]:
        vertices.append(vertices[0])

    for start, end in zip(vertices, vertices[1:]):
        if point_on_segment(point, start, end):
            return True

    point_lon, point_lat = point
    inside = False
    for (lon_a, lat_a), (lon_b, lat_b) in zip(vertices, vertices[1:]):
        intersects = ((lat_a > point_lat) != (lat_b > point_lat)) and (
            point_lon < (lon_b - lon_a) * (point_lat - lat_a) / (lat_b - lat_a) + lon_a
        )
        if intersects:
            inside = not inside
    return inside


def point_on_segment(
    point: tuple[float, float],
    segment_start: tuple[float, float],
    segment_end: tuple[float, float],
    *,
    tolerance: float = 1e-9,
) -> bool:
    point_lon, point_lat = point
    start_lon, start_lat = segment_start
    end_lon, end_lat = segment_end

    cross_product = (
        (point_lat - start_lat) * (end_lon - start_lon)
        - (point_lon - start_lon) * (end_lat - start_lat)
    )
    if abs(cross_product) > tolerance:
        return False

    min_lon = min(start_lon, end_lon) - tolerance
    max_lon = max(start_lon, end_lon) + tolerance
    min_lat = min(start_lat, end_lat) - tolerance
    max_lat = max(start_lat, end_lat) + tolerance
    return min_lon <= point_lon <= max_lon and min_lat <= point_lat <= max_lat
