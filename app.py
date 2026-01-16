import io
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from etl.prepare_data import clean_contacts, clean_disbursements, clean_lines, clean_splaft

BASE_DIR = Path(__file__).parent
SAMPLE_DIR = BASE_DIR / "sample_data"

st.set_page_config(page_title="FMV Tracker", layout="wide")

st.title("FMV Tracker")
st.caption(
    "MVP de portafolio (datos ficticios): seguimiento ESFS/IFI, desembolsos, SPLAFT y contactos. "
    "Ideal para demostrar proactividad + control de bases + reportes."
)


def read_any(uploaded_file) -> pd.DataFrame:
    """Read CSV or Excel from Streamlit uploader."""
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(uploaded_file)
    raise ValueError("Formato no soportado. Sube .csv o .xlsx")


def read_sample(filename: str) -> pd.DataFrame:
    p = SAMPLE_DIR / filename
    if not p.exists():
        raise FileNotFoundError(f"No existe plantilla: {p}")
    return pd.read_csv(p)


# ---------- Sidebar ----------
with st.sidebar:
    st.header("Cargar archivos")
    st.write("Sube tus Excel/CSV. Si no tienes a la mano, usa las plantillas ficticias.")
    use_samples = st.checkbox(
        "Usar datos de ejemplo si no subo archivos",
        value=True,
        key="use_samples_checkbox",
    )

    up_lines = st.file_uploader(
        "1) Líneas ESFS (csv/xlsx)",
        type=["csv", "xlsx", "xls"],
        key="uploader_lines",
    )
    up_disb = st.file_uploader(
        "2) Desembolsos IFI diarios (csv/xlsx)",
        type=["csv", "xlsx", "xls"],
        key="uploader_disb",
    )
    up_splaft = st.file_uploader(
        "3) SPLAFT (csv/xlsx)",
        type=["csv", "xlsx", "xls"],
        key="uploader_splaft",
    )
    up_contacts = st.file_uploader(
        "4) Contactos ESFS/IFI (csv/xlsx)",
        type=["csv", "xlsx", "xls"],
        key="uploader_contacts",
    )

    st.divider()
    st.markdown(
        "**Tip portafolio:** publica solo datos ficticios + capturas del tablero. "
        "No subas información sensible."
    )


# ---------- Load + Clean ----------
lines_df = disb_df = splaft_df = contacts_df = None

try:
    if up_lines is not None:
        lines_df = clean_lines(read_any(up_lines))
    elif use_samples:
        lines_df = clean_lines(read_sample("lines_esfs_template.csv"))

    if up_disb is not None:
        disb_df = clean_disbursements(read_any(up_disb))
    elif use_samples:
        disb_df = clean_disbursements(read_sample("desembolsos_ifi_template.csv"))

    if up_splaft is not None:
        splaft_df = clean_splaft(read_any(up_splaft))
    elif use_samples:
        splaft_df = clean_splaft(read_sample("splaft_template.csv"))

    if up_contacts is not None:
        contacts_df = clean_contacts(read_any(up_contacts))
    elif use_samples:
        contacts_df = clean_contacts(read_sample("contactos_template.csv"))

except Exception as e:
    st.error(f"Error leyendo/limpiando archivos: {e}")


tabs = st.tabs(["Líneas ESFS", "Desembolsos IFI", "SPLAFT", "Contactos", "Exportar"])


# ---------- TAB 1: Lines ----------
with tabs[0]:
    st.subheader("Seguimiento de líneas (ESFS)")

    if lines_df is None or len(lines_df) == 0:
        st.info("Carga el archivo de líneas ESFS para ver este tablero.")
    else:
        df = lines_df.copy()

        monto_aprobado = float(df["monto_aprobado"].sum()) if "monto_aprobado" in df.columns else 0.0
        monto_utilizado = float(df["monto_utilizado"].sum()) if "monto_utilizado" in df.columns else 0.0
        uso_pct_prom = (
            float(df["uso_pct"].mean())
            if "uso_pct" in df.columns and df["uso_pct"].notna().any()
            else 0.0
        )
        n_esfs = df["esfs"].nunique() if "esfs" in df.columns else len(df)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("ESFS", n_esfs)
        c2.metric("Monto aprobado (sum)", f"{monto_aprobado:,.0f}")
        c3.metric("Monto utilizado (sum)", f"{monto_utilizado:,.0f}")
        c4.metric("Uso promedio", f"{uso_pct_prom:,.1f}%")

        # Filters (KEY ÚNICO)
        if "esfs" in df.columns:
            esfs_list = sorted(df["esfs"].dropna().unique().tolist())
            selected = st.multiselect("Filtrar ESFS", esfs_list, key="filter_esfs_lines")
            if selected:
                df = df[df["esfs"].isin(selected)]

        st.dataframe(df, use_container_width=True)

        # Alert: expira en <=30 días
        if "fecha_vigencia" in df.columns and df["fecha_vigencia"].notna().any():
            today = pd.to_datetime(datetime.today().date())
            soon = df[df["fecha_vigencia"].notna() & (df["fecha_vigencia"] <= today + pd.Timedelta(days=30))]
            if len(soon) > 0:
                st.warning("Líneas por vencer en <= 30 días")
                cols = [
                    c for c in
                    ["esfs", "tipo_linea", "monto_aprobado", "saldo_disponible", "fecha_vigencia"]
                    if c in soon.columns
                ]
                st.dataframe(soon[cols], use_container_width=True)


# ---------- TAB 2: Disbursements ----------
with tabs[1]:
    st.subheader("Desembolsos diarios (IFI)")

    if disb_df is None or len(disb_df) == 0:
        st.info("Carga el archivo de desembolsos IFI para ver este tablero.")
    else:
        df = disb_df.copy()
        if "fecha" in df.columns:
            df = df.sort_values("fecha")

        total = float(df["monto_desembolso"].sum()) if "monto_desembolso" in df.columns else 0.0
        last_date = df["fecha"].max().date() if "fecha" in df.columns and df["fecha"].notna().any() else None
        last_day_total = 0.0

        if last_date and "fecha" in df.columns and "monto_desembolso" in df.columns:
            last_day_total = float(df[df["fecha"].dt.date == last_date]["monto_desembolso"].sum())

        last_7_total = 0.0
        if "fecha" in df.columns and df["fecha"].notna().any():
            cutoff = df["fecha"].max() - pd.Timedelta(days=7)
            last_7_total = float(df[df["fecha"] >= cutoff]["monto_desembolso"].sum())

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total desembolsado", f"{total:,.0f}")
        c2.metric("Última fecha", f"{last_date}" if last_date else "-")
        c3.metric("Desembolso última fecha", f"{last_day_total:,.0f}")
        c4.metric("Últimos 7 días", f"{last_7_total:,.0f}")

        # Filters (KEY ÚNICO)
        if "ifi" in df.columns:
            ifi_list = sorted(df["ifi"].dropna().unique().tolist())
            selected = st.multiselect("Filtrar IFI", ifi_list, key="filter_ifi_disb")
            if selected:
                df = df[df["ifi"].isin(selected)]

        st.dataframe(df, use_container_width=True)


# ---------- TAB 3: SPLAFT ----------
with tabs[2]:
    st.subheader("Control documentario SPLAFT")

    if splaft_df is None or len(splaft_df) == 0:
        st.info("Carga el archivo SPLAFT para ver este tablero.")
    else:
        df = splaft_df.copy()

        if "estado" in df.columns:
            status_counts = (
                df["estado"].fillna("(vacío)").astype(str).str.strip().value_counts().reset_index()
            )
            status_counts.columns = ["estado", "cantidad"]
            st.dataframe(status_counts, use_container_width=True)

        # Filters (KEY ÚNICO) — OJO: mismo label que Líneas, pero key distinto
        if "esfs" in df.columns:
            esfs_list = sorted(df["esfs"].dropna().unique().tolist())
            selected = st.multiselect("Filtrar ESFS", esfs_list, key="filter_esfs_splaft")
            if selected:
                df = df[df["esfs"].isin(selected)]

        st.dataframe(df, use_container_width=True)

        if "estado" in df.columns:
            alerts = df[df["estado"].astype(str).str.lower().isin(["pendiente", "observado"])].copy()
            if len(alerts) > 0:
                st.warning("Pendientes / Observados")
                st.dataframe(alerts, use_container_width=True)


# ---------- TAB 4: Contacts ----------
with tabs[3]:
    st.subheader("Contactos ESFS/IFI (calidad de base)")

    if contacts_df is None or len(contacts_df) == 0:
        st.info("Carga el archivo de contactos para ver este tablero.")
    else:
        df = contacts_df.copy()

        missing_email = 0
        if "correo" in df.columns:
            missing_email = int((df["correo"].isna() | (df["correo"].astype(str).str.strip() == "")).sum())

        missing_phone = 0
        if "telefono" in df.columns:
            missing_phone = int((df["telefono"].isna() | (df["telefono"].astype(str).str.strip() == "")).sum())

        duplicates = 0
        if "correo" in df.columns:
            duplicates = int(df["correo"].astype(str).str.strip().duplicated().sum())

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Registros", len(df))
        c2.metric("Sin correo", missing_email)
        c3.metric("Sin teléfono", missing_phone)
        c4.metric("Correos duplicados", duplicates)

        # Filters (KEY ÚNICO)
        if "institucion" in df.columns:
            inst_list = sorted(df["institucion"].dropna().unique().tolist())
            selected = st.multiselect("Filtrar institución", inst_list, key="filter_inst_contacts")
            if selected:
                df = df[df["institucion"].isin(selected)]

        st.dataframe(df, use_container_width=True)


# ---------- TAB 5: Export ----------
with tabs[4]:
    st.subheader("Exportar reporte (Excel)")
    st.write("Genera un Excel con hojas limpias + un resumen rápido.")

    if all(x is None for x in [lines_df, disb_df, splaft_df, contacts_df]):
        st.info("Carga al menos un archivo para habilitar exportación.")
    else:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            summary = []

            if lines_df is not None and len(lines_df) > 0:
                summary.append({
                    "dataset": "lineas",
                    "registros": len(lines_df),
                    "monto_aprobado_sum": float(lines_df["monto_aprobado"].sum()) if "monto_aprobado" in lines_df.columns else None,
                    "uso_pct_prom": float(lines_df["uso_pct"].mean()) if "uso_pct" in lines_df.columns else None,
                })

            if disb_df is not None and len(disb_df) > 0:
                summary.append({
                    "dataset": "desembolsos",
                    "registros": len(disb_df),
                    "monto_desembolso_sum": float(disb_df["monto_desembolso"].sum()) if "monto_desembolso" in disb_df.columns else None,
                })

            if splaft_df is not None and len(splaft_df) > 0:
                summary.append({"dataset": "splaft", "registros": len(splaft_df)})

            if contacts_df is not None and len(contacts_df) > 0:
                summary.append({"dataset": "contactos", "registros": len(contacts_df)})

            pd.DataFrame(summary).to_excel(writer, sheet_name="resumen", index=False)

            if lines_df is not None:
                lines_df.to_excel(writer, sheet_name="lineas", index=False)
            if disb_df is not None:
                disb_df.to_excel(writer, sheet_name="desembolsos", index=False)
            if splaft_df is not None:
                splaft_df.to_excel(writer, sheet_name="splaft", index=False)
            if contacts_df is not None:
                contacts_df.to_excel(writer, sheet_name="contactos", index=False)

        output.seek(0)
        st.download_button(
            label="Descargar fmv_tracker_reporte.xlsx",
            data=output,
            file_name="fmv_tracker_reporte.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_excel",  # KEY ÚNICO
        )
