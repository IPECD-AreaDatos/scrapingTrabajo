import pandas as pd

# Mapeo de nombres de columnas del CSV → nombres en la base de datos
COLUMN_MAPPING = {
    "Fecha":                                                          "fecha",
    "CFI (Neta de Ley 26075)":                                        "cfi_neta_ley_26075",
    "Financ. Educativo (Ley 26075)":                                  "financ_educativo_ley_26075",
    "SUBTOTAL":                                                        "subtotal",
    "Transferencia de Servicios - Educacion":                         "transf_servicios_educacion",
    "Transferencia de Servicios - Posoco":                            "transf_servicios_posoco",
    "Transferencia de Servicios - Prosonu":                           "transf_servicios_prosonu",
    "Transferencia de Servicios - TOTAL":                             "transf_servicios_total",
    "Imp. Bienes Personales (Ley 24.699)":                            "imp_bienes_personales_ley_24699",
    "Imp. Bienes Personales (Ley 23.966 Art. 30)":                    "imp_bienes_personales_ley_23966",
    "Imp. s/ los Activos Fdo. Educativo (Ley 23.906)":               "imp_activos_fdo_educativo",
    "I.V.A. (Ley 23.966 Art. 5 Pto. 2)":                             "iva_ley_23966",
    "Imp. Combustibles (Ley N.23966 Obras de Infraestructura)":       "imp_combustibles_infraestructura",
    "Imp. Combustibles (Ley N.23966 Vialidad Provincial)":            "imp_combustibles_vialidad",
    "Imp. Combustibles (FO.NA.VI.)":                                   "imp_combustibles_fonavi",
    "Fondo Compensador Deseq.Fisc. Provinciales":                     "fondo_compensador_deseq_fisc",
    "Reg.Simplif. p/Pequenos Contribuyentes (Ley N.24.977)":          "reg_simplif_monotributo",
    "TOTAL Recursos Origen Nacional (1)":                              "total_recursos_origen_nacional",
    "Compensacion Consenso Fiscal (2)":                                "compensacion_consenso_fiscal",
    "Total - (1)+(2)":                                                 "total_general",
}

class transformData:
    def processData(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transforma el DataFrame: renombra columnas, parsea fechas y limpia valores."""

        # 1. Renombrar columnas según el mapeo
        df = df.rename(columns=COLUMN_MAPPING)

        # 2. Agregar columnas ausentes del CSV que sí están en la tabla BD
        if "transf_servicios_hospitales" not in df.columns:
            df["transf_servicios_hospitales"] = 0.0
        if "transf_servicios_minoridad" not in df.columns:
            df["transf_servicios_minoridad"] = 0.0

        # 3. Parsear la columna fecha (formato DD/MM/YYYY)
        df["fecha"] = pd.to_datetime(df["fecha"], format="%d/%m/%Y", errors="coerce")

        # 4. Convertir columnas numéricas: rellenar vacíos con 0 y redondear
        numeric_cols = [c for c in df.columns if c != "fecha"]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        df[numeric_cols] = df[numeric_cols].round(2)

        # 5. Eliminar filas con fecha inválida
        filas_antes = len(df)
        df = df.dropna(subset=["fecha"]).reset_index(drop=True)
        filas_despues = len(df)
        if filas_antes != filas_despues:
            print(f"Se descartaron {filas_antes - filas_despues} filas con fecha inválida.")

        # 6. Ordenar columnas en el orden esperado por la tabla
        ordered_cols = [
            "fecha",
            "cfi_neta_ley_26075",
            "financ_educativo_ley_26075",
            "subtotal",
            "transf_servicios_educacion",
            "transf_servicios_posoco",
            "transf_servicios_prosonu",
            "transf_servicios_hospitales",
            "transf_servicios_minoridad",
            "transf_servicios_total",
            "imp_bienes_personales_ley_24699",
            "imp_bienes_personales_ley_23966",
            "imp_activos_fdo_educativo",
            "iva_ley_23966",
            "imp_combustibles_infraestructura",
            "imp_combustibles_vialidad",
            "imp_combustibles_fonavi",
            "fondo_compensador_deseq_fisc",
            "reg_simplif_monotributo",
            "total_recursos_origen_nacional",
            "compensacion_consenso_fiscal",
            "total_general",
        ]
        df = df[ordered_cols]

        print(f"Transformación completada. Filas resultantes: {len(df)}")
        return df
