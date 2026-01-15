# FMV Tracker (MVP de Portafolio)

**Propuesta de valor:** Tablero simple (Streamlit) que consolida archivos Excel/CSV para:
- Seguimiento de **líneas ESFS** (monto, saldo, % uso, alertas por vencimiento)
- Consolidación de **desembolsos diarios IFI**
- Control documentario **SPLAFT** (pendiente/recibido/observado/aprobado)
- Calidad de base de **contactos ESFS/IFI** (faltantes y duplicados)

> Nota de privacidad: publica en GitHub solo datos ficticios/anónimos + capturas.

## Ejecutar localmente
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# Mac/Linux: source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
