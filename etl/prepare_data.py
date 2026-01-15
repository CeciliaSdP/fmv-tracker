"""
FMV Tracker - ETL simple (portafolio)

Objetivo:
- normalizar nombres de columnas
- limpiar texto (instituciones)
- convertir fechas y números
- crear campos derivados (monto_utilizado, uso_pct)

Tip portafolio:
- Publica SOLO datos ficticios/anónimos.
"""

from __future__ import annotations

import re
from typing import Dict

import pandas as pd


def normalize_colname(name: str) -> str:
    name = str(name).strip().lower()
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^a-z0-9_]+", "", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [normalize_colname(c) for c in df.columns]
    return df


def apply_aliases(df: pd.DataFrame, aliases: Dict[str, str]) -> pd.DataFrame:
    """
    Renombra columnas usando aliases.
    Ejemplo: {"entidad": "esfs", "institucion_financiera": "ifi"}
    """
    df = normalize_columns(df)
    rename_map = {normalize_colname(k): normalize_colname(v) for k, v in aliases.items()}
    return df.rename(columns={c: rename_map.get(c, c) for c in df.columns})


def clean_text(df: pd.DataFrame, col: str, upper: bool = False) -> pd.DataFrame:
    df = df.copy()
    if col not in df.columns:
        return df
    s = df[col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    if upper:
        s = s.str.upper()
    df[col] = s
    return df


def coerce_date(df: pd.DataFrame, col: str) -> pd.DataFrame:
    df = df.copy()
    if col not in df.columns:
        return df
    df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def to_number(series: pd.Series) -> pd.Series:
    """
    Convierte textos como 'S/ 1,230.50' o '1 230,50' a número.
    - Elimina moneda y espacios
    - Soporta coma como separador decimal en algunos casos (heurística simple)
    """
    s = series.astype(str).str.strip()

    # elimina moneda y caracteres raros, conserva dígitos, punto, coma, signo
    s = s.str.replace(r"[^\d\-,\.]", "", regex=True)

    # heurística:
    # si hay coma y punto, asumimos que coma es miles y punto decimal (ej: 1,234.56)
    # si solo hay coma, asumimos coma decimal (ej: 1234,56)
    has_comma = s.str.contains(",", regex=False)
    has_dot = s.str.contains(".", regex=False)

    # caso solo coma -> coma decimal
    only_comma = has_comma & ~has_dot
    s.loc[only_comma] = s.loc[only_comma].str.replace(".", "", regex=False)
    s.loc[only_comma] = s.loc[only_comma].str.replace(",", ".", regex=False)

    # caso coma y punto -> quitar comas (miles)
    both = has_comma & has_dot
    s.loc[both] = s.loc[both].str.replace(",", "", regex=False)

    return pd.to_numeric(s, errors="coerce")


# ----------------- Cleaners -----------------

def clean_lines(df: pd.DataFrame) -> pd.DataFrame:
    """
    Líneas ESFS (flexible):
    - esfs
    - tipo_linea
    - monto_aprobado
    - saldo_disponible (o monto_utilizado)
    - fecha_vigencia (opcional)
    """
    aliases = {
        "entidad": "esfs",
        "institucion": "esfs",
        "banco": "esfs",
        "tipo": "tipo_linea",
        "linea": "tipo_linea",
        "monto": "monto_aprobado",
        "monto_linea": "monto_aprobado",
        "saldo": "saldo_disponible",
        "saldo_linea": "saldo_disponible",
        "vigencia": "fecha_vigencia",
        "fecha_vencimiento": "fecha_vigencia",
    }
    df = apply_aliases(df, aliases)

    df = clean_text(df, "esfs", upper=True)
    df = clean_text(df, "tipo_linea", upper=False)
    df = coerce_date(df, "fecha_vigencia")

    for c in ["monto_aprobado", "saldo_disponible", "monto_utilizado"]:
        if c in df.columns:
            df[c] = to_number(df[c])

    # derivados
    if "monto_aprobado" in df.columns:
        if "saldo_disponible" in df.columns and "monto_utilizado" not in df.columns:
            df["monto_utilizado"] = df["monto_aprobado"] - df["saldo_disponible"]

        if "monto_utilizado" in df.columns:
            df["uso_pct"] = (df["monto_utilizado"] / df["monto_aprobado"]) * 100

    return df


def clean_disbursements(df: pd.DataFrame) -> pd.DataFrame:
    """
    Desembolsos IFI:
    - ifi
    - fecha
    - monto_desembolso
    """
    aliases = {
        "institucion": "ifi",
        "institucion_financiera": "ifi",
        "monto": "monto_desembolso",
        "desembolso": "monto_desembolso",
        "importe": "monto_desembolso",
    }
    df = apply_aliases(df, aliases)

    df = clean_text(df, "ifi", upper=True)
    df = coerce_date(df, "fecha")
    if "monto_desembolso" in df.columns:
        df["monto_desembolso"] = to_number(df["monto_desembolso"])
    return df


def clean_splaft(df: pd.DataFrame) -> pd.DataFrame:
    """
    SPLAFT:
    - esfs
    - documento
    - estado (pendiente/recibido/observado/aprobado)
    - fecha_actualizacion (opcional)
    """
    aliases = {
        "entidad": "esfs",
        "institucion": "esfs",
        "fecha": "fecha_actualizacion",
        "actualizacion": "fecha_actualizacion",
    }
    df = apply_aliases(df, aliases)

    df = clean_text(df, "esfs", upper=True)
    df = clean_text(df, "documento", upper=False)
    df = clean_text(df, "estado", upper=False)
    df = coerce_date(df, "fecha_actualizacion")

    # estandariza estado básico
    if "estado" in df.columns:
        s = df["estado"].astype(str).str.strip().str.lower()
        s = s.replace({
            "enviado": "recibido",
            "ok": "aprobado",
            "aprobada": "aprobado",
            "observada": "observado",
        })
        df["estado"] = s

    return df


def clean_contacts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Contactos:
    - institucion
    - nombre
    - cargo
    - correo
    - telefono
    - ultima_actualizacion (opcional)
    """
    aliases = {
        "entidad": "institucion",
        "esfs": "institucion",
        "ifi": "institucion",
        "mail": "correo",
        "email": "correo",
        "celular": "telefono",
        "telefono_contacto": "telefono",
        "fecha": "ultima_actualizacion",
    }
    df = apply_aliases(df, aliases)

    df = clean_text(df, "institucion", upper=True)
    for c in ["nombre", "cargo", "correo", "telefono"]:
        df = clean_text(df, c, upper=False)

    df = coerce_date(df, "ultima_actualizacion")
    return df
