from __future__ import annotations

import hashlib
import json
import math
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
import re
from zoneinfo import ZoneInfo

from PIL import Image, ImageDraw, ImageFilter, ImageFont
from rasterio.warp import transform as transform_points
from rasterio.warp import transform_bounds
from rasterio.windows import bounds as window_bounds
from rasterio.windows import Window

from goes_timelapse.catalog import load_boundary_lines
from goes_timelapse.config import Settings
from goes_timelapse.models import AreaCatalogEntry, AreaGeometry, BoundaryLine
from goes_timelapse.raster_sources import WGS84_CRS, open_raster_source


BRAZIL_TZ = ZoneInfo("America/Sao_Paulo")
LEGACY_TIMESTAMP_PATTERN = re.compile(r"^(\d{11})_")
NETCDF_TIMESTAMP_PATTERN = re.compile(r"_s(\d{13,14})_")
LOGGER = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class RenderPlan:
    window: Window
    dst_bounds: tuple[float, float, float, float]
    scaled_polygon: tuple[tuple[float, float], ...]
    scaled_state_lines: tuple[tuple[tuple[float, float], ...], ...]
    output_size: tuple[int, int]
    scaled_marker: tuple[float, float] | None = None


@dataclass(slots=True, frozen=True)
class FrameSpec:
    timestamp: str
    primary_path: Path
    blend_path: Path | None = None
    blend_alpha: float = 0.0
    lightning_points: tuple[tuple[float, float], ...] = ()


def parse_goes_timestamp(filename: str) -> str:
    name = Path(filename).name
    legacy_match = LEGACY_TIMESTAMP_PATTERN.match(name)
    if legacy_match:
        return legacy_match.group(1)

    netcdf_match = NETCDF_TIMESTAMP_PATTERN.search(name)
    if netcdf_match:
        return netcdf_match.group(1)[:11]

    return name.split("_")[0]


def format_capture_time(filename: str) -> str:
    timestamp = parse_goes_timestamp(filename)
    if len(timestamp) < 11:
        return "Unknown capture time"

    year = int(timestamp[0:4])
    day_of_year = int(timestamp[4:7])
    hour = int(timestamp[7:9])
    minute = int(timestamp[9:11])
    capture_date = datetime(year, 1, 1, hour=hour, minute=minute, tzinfo=UTC)
    capture_date = capture_date + timedelta(days=day_of_year - 1)
    local_time = capture_date.astimezone(BRAZIL_TZ)
    return local_time.strftime("%d/%m/%Y %H:%M")


class AreaRenderer:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._font_cache: dict[int, ImageFont.FreeTypeFont | ImageFont.ImageFont] = {}
        try:
            self._state_boundary_lines: tuple[BoundaryLine, ...] = load_boundary_lines(
                settings.state_boundaries_path
            )
        except FileNotFoundError:
            self._state_boundary_lines = ()

    def process_frames(
        self,
        area: AreaCatalogEntry,
        geometry: AreaGeometry,
        frame_inputs: list[Path | FrameSpec],
        *,
        marker_coordinates: tuple[float, float] | None = None,
    ) -> list[Path]:
        frame_specs = self._coerce_frame_specs(frame_inputs)[-self._settings.frame_count :]
        if not frame_specs:
            return []

        output_dir = self._settings.processed_dir / area.area_id
        output_dir.mkdir(parents=True, exist_ok=True)

        with _open_source(frame_specs[0].primary_path) as src:
            plan = self._build_render_plan(geometry, src, marker_coordinates=marker_coordinates)

        rendered_paths: list[Path] = []
        keep_filenames = set()
        for frame_spec in frame_specs:
            png_path = output_dir / self._frame_output_name(frame_spec, plan)
            keep_filenames.add(png_path.name)
            if not png_path.exists():
                self._render_frame(frame_spec, png_path, area, plan)
            rendered_paths.append(png_path)

        for stale_path in output_dir.glob("*.png"):
            if stale_path.name not in keep_filenames:
                stale_path.unlink(missing_ok=True)

        return rendered_paths

    def _frame_output_name(self, frame_spec: FrameSpec, plan: RenderPlan) -> str:
        return f"{frame_spec.timestamp}-{self._frame_cache_key(frame_spec, plan)}.png"

    def _frame_cache_key(self, frame_spec: FrameSpec, plan: RenderPlan) -> str:
        payload = {
            "timestamp": frame_spec.timestamp,
            "primary": frame_spec.primary_path.name,
            "blend": frame_spec.blend_path.name if frame_spec.blend_path is not None else None,
            "blend_alpha": round(frame_spec.blend_alpha, 4),
            "dst_bounds": [round(value, 6) for value in plan.dst_bounds],
            "output_size": list(plan.output_size),
            "marker": (
                [round(plan.scaled_marker[0], 2), round(plan.scaled_marker[1], 2)]
                if plan.scaled_marker is not None
                else None
            ),
            "lightning_points": [
                [round(point[0], 4), round(point[1], 4)]
                for point in frame_spec.lightning_points
            ],
        }
        digest = hashlib.sha1(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return digest[:12]

    def cleanup(self, area_id: str) -> None:
        output_dir = self._settings.processed_dir / area_id
        if not output_dir.exists():
            return
        for file_path in output_dir.glob("*"):
            file_path.unlink(missing_ok=True)
        output_dir.rmdir()

    def _build_render_plan(
        self,
        geometry: AreaGeometry,
        src,
        *,
        marker_coordinates: tuple[float, float] | None = None,
    ) -> RenderPlan:
        dst_bounds = self._fit_destination_bounds_to_source(
            self._build_destination_bounds(geometry),
            src,
        )
        window = self._build_source_window(src, dst_bounds)
        crop_width = int(window.width)
        crop_height = int(window.height)

        minimum_output_dimension = min(600, self._settings.max_render_dimension)
        if max(crop_width, crop_height) <= 360:
            minimum_output_dimension = min(720, self._settings.max_render_dimension)
        target_dimension = min(
            self._settings.max_render_dimension,
            max(max(crop_width, crop_height), minimum_output_dimension),
        )
        output_width = target_dimension
        output_height = target_dimension
        scaled_polygon = self._scale_polygon_to_output(
            geometry.polygon,
            dst_bounds,
            output_width,
            output_height,
        )

        scaled_state_lines = self._scale_state_boundary_lines(
            dst_bounds=dst_bounds,
            output_width=output_width,
            output_height=output_height,
        )
        scaled_marker = self._scale_marker_to_output(
            geometry=geometry,
            marker_coordinates=marker_coordinates,
            dst_bounds=dst_bounds,
            output_width=output_width,
            output_height=output_height,
        )

        return RenderPlan(
            window=window,
            dst_bounds=dst_bounds,
            scaled_polygon=tuple(scaled_polygon),
            scaled_state_lines=tuple(scaled_state_lines),
            output_size=(output_width, output_height),
            scaled_marker=scaled_marker,
        )

    def _fit_destination_bounds_to_source(
        self,
        dst_bounds: tuple[float, float, float, float],
        src,
    ) -> tuple[float, float, float, float]:
        source_left, source_bottom, source_right, source_top = self._source_bounds_in_wgs84(src)
        left, bottom, right, top = dst_bounds
        width = right - left
        height = top - bottom
        source_width = source_right - source_left
        source_height = source_top - source_bottom

        if width >= source_width:
            left, right = source_left, source_right
        else:
            if left < source_left:
                shift = source_left - left
                left += shift
                right += shift
            if right > source_right:
                shift = right - source_right
                left -= shift
                right -= shift

        if height >= source_height:
            bottom, top = source_bottom, source_top
        else:
            if bottom < source_bottom:
                shift = source_bottom - bottom
                bottom += shift
                top += shift
            if top > source_top:
                shift = top - source_top
                bottom -= shift
                top -= shift

        return (
            max(left, source_left),
            max(bottom, source_bottom),
            min(right, source_right),
            min(top, source_top),
        )

    def _render_frame(
        self,
        frame_spec: FrameSpec,
        png_path: Path,
        area: AreaCatalogEntry,
        plan: RenderPlan,
    ) -> None:
        image = self._load_frame_image(frame_spec.primary_path, area.area_id, plan)
        blend_image: Image.Image | None = None
        if frame_spec.blend_path is not None:
            blend_image = self._load_frame_image(frame_spec.blend_path, area.area_id, plan)
            image = Image.blend(image, blend_image, frame_spec.blend_alpha)
            blend_image.close()

        draw = ImageDraw.Draw(image, "RGBA")
        for state_line in plan.scaled_state_lines:
            if len(state_line) >= 2:
                draw.line(state_line, fill=(255, 255, 255, 192), width=1)

        polygon_points = [
            (round(point[0], 1), round(point[1], 1)) for point in plan.scaled_polygon
        ]
        if polygon_points:
            draw.line(polygon_points + [polygon_points[0]], fill=(255, 214, 10, 255), width=2)

        lightning_points = self._scale_points_to_output(
            frame_spec.lightning_points,
            plan.dst_bounds,
            image.size[0],
            image.size[1],
        )
        if lightning_points:
            lightning_radius = max(2, int(round(image.size[0] * 0.0045)))
            for lightning_x, lightning_y in lightning_points:
                draw.ellipse(
                    (
                        lightning_x - lightning_radius,
                        lightning_y - lightning_radius,
                        lightning_x + lightning_radius,
                        lightning_y + lightning_radius,
                    ),
                    fill=(42, 121, 255, 255),
                    outline=(255, 255, 255, 180),
                    width=1,
                )

        if plan.scaled_marker is not None:
            marker_x, marker_y = plan.scaled_marker
            marker_radius = max(4, int(round(image.size[0] * 0.008)))
            draw.ellipse(
                (
                    marker_x - marker_radius,
                    marker_y - marker_radius,
                    marker_x + marker_radius,
                    marker_y + marker_radius,
                ),
                fill=(220, 42, 42, 255),
                outline=(255, 255, 255, 220),
                width=1,
            )

        self._draw_overlay(
            draw,
            image.size,
            area.display_name,
            format_capture_time(frame_spec.primary_path.name),
        )

        temporary_path = png_path.with_suffix(".png.tmp")
        image.convert("RGB").save(temporary_path, format="PNG")
        temporary_path.replace(png_path)
        image.close()

    def _load_frame_image(
        self,
        raw_path: Path,
        area_id: str,
        plan: RenderPlan,
    ) -> Image.Image:
        with _open_source(raw_path) as src:
            image = src.read_image(
                plan.window,
                output_size=plan.output_size,
                dst_bounds=plan.dst_bounds,
            )
        if image.size[0] <= 0 or image.size[1] <= 0:
            LOGGER.warning(
                "Raw frame %s produced an empty crop for %s; using a blank fallback frame",
                raw_path.name,
                area_id,
            )
            image = Image.new("RGBA", plan.output_size, (0, 0, 0, 255))
        if image.size != plan.output_size:
            width_scale = plan.output_size[0] / image.size[0]
            height_scale = plan.output_size[1] / image.size[1]
            upscale_factor = max(width_scale, height_scale)
            resample = (
                Image.Resampling.BICUBIC
                if upscale_factor > 1.0
                else Image.Resampling.LANCZOS
            )
            image = image.resize(plan.output_size, resample)
            if upscale_factor >= 1.75:
                image = image.filter(ImageFilter.GaussianBlur(radius=0.55))
                image = image.filter(
                    ImageFilter.UnsharpMask(radius=0.9, percent=70, threshold=3)
                )
            elif upscale_factor > 1.0:
                image = image.filter(ImageFilter.GaussianBlur(radius=0.3))
                image = image.filter(
                    ImageFilter.UnsharpMask(radius=0.8, percent=55, threshold=2)
                )
            else:
                image = image.filter(
                    ImageFilter.UnsharpMask(radius=1.0, percent=110, threshold=2)
                )
        return image

    def _draw_overlay(
        self,
        draw: ImageDraw.ImageDraw,
        image_size: tuple[int, int],
        area_name: str,
        capture_time: str,
    ) -> None:
        font_size = max(16, int(image_size[0] * 0.03))
        font = self._load_font(font_size)
        lines = [area_name, capture_time]
        boxes = [draw.textbbox((0, 0), line, font=font) for line in lines]
        text_width = max(box[2] - box[0] for box in boxes)
        line_gap = 6
        padding_x = 14
        padding_y = 10
        text_height = sum(box[3] - box[1] for box in boxes) + line_gap * (len(lines) - 1)
        background_box = (
            20,
            20,
            20 + text_width + padding_x * 2,
            20 + text_height + padding_y * 2,
        )
        draw.rounded_rectangle(background_box, radius=12, fill=(0, 0, 0, 150))
        y_position = background_box[1] + padding_y
        for line, box in zip(lines, boxes, strict=True):
            draw.text(
                (background_box[0] + padding_x, y_position),
                line,
                fill=(255, 255, 255, 255),
                font=font,
            )
            y_position += box[3] - box[1] + line_gap

    def _load_font(self, font_size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
        cached = self._font_cache.get(font_size)
        if cached is not None:
            return cached
        try:
            font = ImageFont.truetype(str(self._settings.font_path), font_size)
        except OSError:
            font = ImageFont.load_default()
        self._font_cache[font_size] = font
        return font

    def _scale_state_boundary_lines(
        self,
        *,
        dst_bounds: tuple[float, float, float, float],
        output_width: int,
        output_height: int,
    ) -> list[tuple[tuple[float, float], ...]]:
        scaled_lines: list[tuple[tuple[float, float], ...]] = []
        for boundary_line in self._state_boundary_lines:
            scaled_line = self._scale_polygon_to_output(
                boundary_line.line,
                dst_bounds,
                output_width,
                output_height,
            )
            if len(scaled_line) < 2:
                continue
            xs = [point[0] for point in scaled_line]
            ys = [point[1] for point in scaled_line]
            if max(xs) < 0 or min(xs) > output_width:
                continue
            if max(ys) < 0 or min(ys) > output_height:
                continue
            scaled_lines.append(tuple(scaled_line))
        return scaled_lines

    def _project_polygon_to_source(self, polygon: tuple[tuple[float, float], ...], src) -> list[tuple[float, float]]:
        return self._project_points_to_source(polygon, src)

    def _project_points_to_source(
        self,
        points: tuple[tuple[float, float], ...],
        src,
    ) -> list[tuple[float, float]]:
        if not points:
            return []
        inverse_transform = ~src.transform
        if src.crs == WGS84_CRS:
            return [inverse_transform * point for point in points]

        lon_values = [point[0] for point in points]
        lat_values = [point[1] for point in points]
        x_values, y_values = transform_points(WGS84_CRS, src.crs, lon_values, lat_values)
        return [inverse_transform * (x, y) for x, y in zip(x_values, y_values, strict=True)]

    def _build_destination_bounds(self, geometry: AreaGeometry) -> tuple[float, float, float, float]:
        min_lon, min_lat, max_lon, max_lat = geometry.bounds
        centroid_lon, centroid_lat = geometry.centroid
        base_span = max(max_lon - min_lon, max_lat - min_lat)
        if base_span <= 0.18:
            span = base_span * 5.0
        elif base_span <= 0.35:
            span = base_span * 4.4
        elif base_span <= 0.7:
            span = base_span * 3.7
        elif base_span <= 1.4:
            span = base_span * 2.8
        else:
            span = base_span * 2.1

        span = max(span, 1.7)
        half_span = span / 2.0
        return (
            centroid_lon - half_span,
            centroid_lat - half_span,
            centroid_lon + half_span,
            centroid_lat + half_span,
        )

    def _build_source_window(
        self,
        src,
        dst_bounds: tuple[float, float, float, float],
    ) -> Window:
        if src.crs == WGS84_CRS:
            source_bounds = dst_bounds
        else:
            source_bounds = self._transform_bounds_to_source(dst_bounds, src.crs)

        min_x, min_y, max_x, max_y = source_bounds
        inverse_transform = ~src.transform
        left_col, top_row = inverse_transform * (min_x, max_y)
        right_col, bottom_row = inverse_transform * (max_x, min_y)

        margin = 24
        col_off = int(math.floor(min(left_col, right_col))) - margin
        row_off = int(math.floor(min(top_row, bottom_row))) - margin
        col_max = int(math.ceil(max(left_col, right_col))) + margin
        row_max = int(math.ceil(max(top_row, bottom_row))) + margin

        col_off = min(max(col_off, 0), max(src.width - 1, 0))
        row_off = min(max(row_off, 0), max(src.height - 1, 0))
        col_max = min(max(col_max, col_off + 1), src.width)
        row_max = min(max(row_max, row_off + 1), src.height)
        return Window(
            col_off=col_off,
            row_off=row_off,
            width=col_max - col_off,
            height=row_max - row_off,
        )

    def _transform_bounds_to_source(
        self,
        bounds: tuple[float, float, float, float],
        dst_crs,
    ) -> tuple[float, float, float, float]:
        left, bottom, right, top = bounds
        samples = []
        steps = 8
        for index in range(steps + 1):
            ratio = index / steps
            lon = left + (right - left) * ratio
            lat = bottom + (top - bottom) * ratio
            samples.append((lon, top))
            samples.append((lon, bottom))
            samples.append((left, lat))
            samples.append((right, lat))
        x_values, y_values = transform_points(
            WGS84_CRS,
            dst_crs,
            [point[0] for point in samples],
            [point[1] for point in samples],
        )
        return (min(x_values), min(y_values), max(x_values), max(y_values))

    def _source_bounds_in_wgs84(self, src) -> tuple[float, float, float, float]:
        source_bounds = window_bounds(
            Window(col_off=0, row_off=0, width=src.width, height=src.height),
            src.transform,
        )
        if src.crs == WGS84_CRS:
            return source_bounds
        return transform_bounds(src.crs, WGS84_CRS, *source_bounds, densify_pts=21)

    def _scale_polygon_to_output(
        self,
        points: tuple[tuple[float, float], ...],
        dst_bounds: tuple[float, float, float, float],
        output_width: int,
        output_height: int,
    ) -> list[tuple[float, float]]:
        if not points:
            return []
        left, bottom, right, top = dst_bounds
        width = max(right - left, 1e-9)
        height = max(top - bottom, 1e-9)
        scaled: list[tuple[float, float]] = []
        for lon, lat in points:
            x = ((lon - left) / width) * output_width
            y = ((top - lat) / height) * output_height
            scaled.append((x, y))
        return scaled

    def _scale_points_to_output(
        self,
        points: tuple[tuple[float, float], ...],
        dst_bounds: tuple[float, float, float, float],
        output_width: int,
        output_height: int,
    ) -> list[tuple[float, float]]:
        scaled_points = self._scale_polygon_to_output(
            points,
            dst_bounds,
            output_width,
            output_height,
        )
        return [
            (point_x, point_y)
            for point_x, point_y in scaled_points
            if 0 <= point_x <= output_width and 0 <= point_y <= output_height
        ]

    def _scale_marker_to_output(
        self,
        *,
        geometry: AreaGeometry,
        marker_coordinates: tuple[float, float] | None,
        dst_bounds: tuple[float, float, float, float],
        output_width: int,
        output_height: int,
    ) -> tuple[float, float] | None:
        if marker_coordinates is None:
            return None

        scaled = self._scale_polygon_to_output(
            (marker_coordinates,),
            dst_bounds,
            output_width,
            output_height,
        )
        if not scaled:
            return None

        marker_x, marker_y = scaled[0]
        if not (0 <= marker_x <= output_width and 0 <= marker_y <= output_height):
            LOGGER.warning(
                "Marker for %s fell outside render bounds; skipping marker overlay",
                geometry.area_id,
            )
            return None

        return (marker_x, marker_y)

    @staticmethod
    def _coerce_frame_specs(frame_inputs: list[Path | FrameSpec]) -> list[FrameSpec]:
        frame_specs: list[FrameSpec] = []
        for item in frame_inputs:
            if isinstance(item, FrameSpec):
                frame_specs.append(item)
            else:
                frame_specs.append(
                    FrameSpec(
                        timestamp=parse_goes_timestamp(item.name),
                        primary_path=item,
                    )
                )
        frame_specs.sort(key=lambda item: item.timestamp)
        return frame_specs


class WebpBuilder:
    def __init__(self, settings: Settings):
        self._settings = settings

    def build(self, area_id: str, png_paths: list[Path]) -> Path:
        output_path = self._settings.media_dir / f"{area_id}.webp"
        duration_ms = max(100, round(1000 / self._settings.gif_fps))
        frames = [Image.open(path).convert("RGBA") for path in png_paths]
        if not frames:
            raise ValueError(f"No rendered PNG frames found for {area_id}")

        sequence = frames + [frames[-1].copy() for _ in range(self._settings.gif_fps)]
        temporary_path = output_path.with_suffix(".webp.tmp")
        sequence[0].save(
            temporary_path,
            save_all=True,
            append_images=sequence[1:],
            duration=duration_ms,
            loop=0,
            lossless=False,
            quality=90,
            method=6,
            format="WEBP",
        )
        temporary_path.replace(output_path)

        for frame in frames:
            frame.close()
        for frame in sequence[len(frames) :]:
            frame.close()
        return output_path


GifBuilder = WebpBuilder


def write_lovelace_snippet(
    snippets_dir: Path, area: AreaCatalogEntry, area_id: str
) -> Path:
    snippet_path = snippets_dir / f"{area_id}.yaml"
    snippet = (
        f"# {area.display_name}\n"
        "type: picture\n"
        f"image: media-source://media_source/local/goes_timelapse/{area_id}.webp\n"
        "tap_action:\n"
        "  action: none\n"
        "hold_action:\n"
        "  action: none\n"
    )
    temporary_path = snippet_path.with_suffix(".yaml.tmp")
    temporary_path.write_text(snippet, encoding="utf-8")
    temporary_path.replace(snippet_path)
    return snippet_path


MunicipalityRenderer = AreaRenderer

class _open_source:
    def __init__(self, path: Path):
        self._path = path
        self._source = None

    def __enter__(self):
        self._source = open_raster_source(self._path)
        return self._source

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._source is not None:
            self._source.close()
