from __future__ import annotations

from goes_timelapse.pipeline.runtime_types import SourceDownloadStatus

def build_source_download_summary(
    status: SourceDownloadStatus,
    *,
    raw_frame_count: int,
) -> str:
    phase = status["phase"] or "idle"
    attempted = status["attempted_count"]
    completed = status["completed_count"]
    failed = status["failed_count"]
    active = status["active_count"]
    reason = status["schedule_reason"] or ""

    if phase == "disabled":
        return reason or "Fonte desativada"
    if phase == "paused":
        return reason or "Fonte pausada"
    if phase == "downloading":
        detail = f"{completed}/{attempted}" if attempted else "iniciando"
        return f"Baixando ({detail}, {active} ativos)"
    if phase == "processing":
        detail = f"{completed}/{attempted}" if attempted else "iniciando"
        return f"Convertendo ({detail}, {active} ativos)"
    if phase == "partial":
        if reason:
            return f"{raw_frame_count} arquivo(s) em disco; {reason.lower()}"
        return f"{raw_frame_count} arquivo(s) em disco; {failed} falha(s) no último ciclo"
    if phase == "ready":
        return f"{raw_frame_count} arquivo(s) brutos em disco"
    if phase == "error":
        return reason or f"Falha no download; {failed} erro(s)"
    if raw_frame_count:
        return f"{raw_frame_count} arquivo(s) brutos em disco"
    return reason or "Aguardando primeiro download"


def build_initial_download_status(source_key: str) -> SourceDownloadStatus:
    return {
        "source_key": source_key,
        "phase": "disabled",
        "attempted_count": 0,
        "completed_count": 0,
        "failed_count": 0,
        "active_count": 0,
        "current_file": None,
        "last_downloaded": None,
        "latest_available": None,
        "active_downloads": [],
        "schedule_reason": "Aguardando municípios acompanhados",
        "is_relevant": False,
    }


def download_phase_label(phase: str) -> str:
    mapping = {
        "disabled": "desativado",
        "paused": "pausado",
        "idle": "ocioso",
        "downloading": "baixando",
        "processing": "convertendo",
        "partial": "parcial",
        "ready": "pronto",
        "error": "erro",
    }
    return mapping.get(phase, phase)


def pluralize_slot(count: int) -> str:
    return "slot" if count == 1 else "slots"
