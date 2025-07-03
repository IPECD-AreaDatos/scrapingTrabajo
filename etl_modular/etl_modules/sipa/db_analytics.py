from sqlalchemy import create_engine
import pandas as pd
from numpy import trunc

class SipaAnalytics:
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        self.dwh = "dwh_economico"
        self.datalake = "datalake_economico"  # cambia si quieres

    def generar_analytics(self):
        print("🔄 Generando tablas de análisis SIPA...")

        self._tabla_nacional()
        self._tabla_nea()

        print("✅ Tablas SIPA generadas.")

    def _tabla_nacional(self):
        engine_dl = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.datalake}")
        df_bdd = pd.read_sql("SELECT * FROM sipa_valores WHERE id_provincia=1", engine_dl)

        df = pd.DataFrame()
        df["fecha"] = df_bdd[df_bdd["id_tipo_registro"] == 8]["fecha"].reset_index(drop=True)
        df["empleo_total"] = (
            df_bdd[df_bdd["id_tipo_registro"] == 8]["cantidad_con_estacionalidad"]
            .reset_index(drop=True)
            .apply(lambda x: trunc(x * 1000) / 1000)
        )
        for col, tipo in zip(
            ["empleo_privado", "empleo_publico", "empleo_casas_particulares",
            "empleo_independiente_autonomo", "empleo_independiente_monotributo", "empleo_monotributo_social"],
            [2, 3, 4, 5, 6, 7]
        ):
            valores = df_bdd[df_bdd["id_tipo_registro"] == tipo]["cantidad_con_estacionalidad"].reset_index(drop=True)
            df[col] = valores
            df[f"p_{col}"] = (valores * 100) / df["empleo_total"]

        # Variaciones y diferencias
        df["vmensual_empleo_total"] = df["empleo_total"].pct_change() * 100
        df["vinter_empleo_total"] = (df["empleo_total"] / df["empleo_total"].shift(12) - 1) * 100
        df["dif_mensual_empleo_total"] = df["empleo_total"].diff() * 1000
        df["dif_inter_empleo_total"] = (df["empleo_total"] - df["empleo_total"].shift(12)) * 1000

        df["vmensual_empleo_privado"] = df["empleo_privado"].pct_change() * 100
        df["vinter_empleo_privado"] = (df["empleo_privado"] / df["empleo_privado"].shift(12) - 1) * 100
        df["dif_mensual_empleo_privado"] = df["empleo_privado"].diff() * 1000
        df["dif_inter_empleo_privado"] = (df["empleo_privado"] - df["empleo_privado"].shift(12)) * 1000

        # Variación acumulada inicializada como NaN
        df["vacum_empleo_total"] = float("nan")
        df["vacum_empleo_privado"] = float("nan")
        df["dif_acum_empleo_total"] = float("nan")
        df["dif_acum_empleo_privado"] = float("nan")

        # Asegurar tipo datetime
        df["fecha"] = pd.to_datetime(df["fecha"])

        # Cálculo de acumuladas por año
        for anio in sorted(df["fecha"].dt.year.unique()):
            diciembre = df[(df["fecha"].dt.year == anio - 1) & (df["fecha"].dt.month == 12)]
            if not diciembre.empty:
                total = diciembre["empleo_total"].values[0]
                privado = diciembre["empleo_privado"].values[0]
                mask = df["fecha"].dt.year == anio
                df.loc[mask, "vacum_empleo_total"] = (df.loc[mask, "empleo_total"] / total - 1) * 100
                df.loc[mask, "vacum_empleo_privado"] = (df.loc[mask, "empleo_privado"] / privado - 1) * 100
                df.loc[mask, "dif_acum_empleo_total"] = (df.loc[mask, "empleo_total"] - total) * 1000
                df.loc[mask, "dif_acum_empleo_privado"] = (df.loc[mask, "empleo_privado"] - privado) * 1000

        # Guardar
        engine_dwh = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.dwh}")
        df.to_sql("empleo_nacional_porcentajes_variaciones", engine_dwh, if_exists="replace", index=True)


    def _tabla_nea(self):
        engine_dl = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.datalake}")
        query = """
        SELECT fecha, id_provincia, cantidad_con_estacionalidad 
        FROM sipa_valores 
        WHERE id_provincia IN (18, 22, 34, 54)
        """
        df_bdd = pd.read_sql(query, engine_dl)

        df = pd.DataFrame()
        provincias = {18: "corrientes", 22: "chaco", 34: "formosa", 54: "misiones"}

        # Fechas ordenadas
        fechas_ordenadas = sorted(set(pd.to_datetime(df_bdd["fecha"])))
        df["fecha"] = fechas_ordenadas

        # Cargar totales por provincia
        for idp, nombre in provincias.items():
            df[f"total_{nombre}"] = (
                df_bdd[df_bdd["id_provincia"] == idp]["cantidad_con_estacionalidad"]
                .reset_index(drop=True) * 1000
            )

        # Sumar total NEA
        df["total_nea"] = sum(df[f"total_{prov}"] for prov in provincias.values())

        # Inicializar columnas acumuladas como NaN
        for prov in list(provincias.values()) + ["nea"]:
            df[f"vacum_{prov}"] = float("nan")
            df[f"dif_mensual_{prov}"] = float("nan")
            df[f"dif_inter_{prov}"] = float("nan")
            df[f"dif_acum_{prov}"] = float("nan")

        # Calcular variaciones y diferencias
        for prov in list(provincias.values()) + ["nea"]:
            total = df[f"total_{prov}"]

            df[f"vmensual_{prov}"] = total.pct_change() * 100
            df[f"vinter_{prov}"] = (total / total.shift(12) - 1) * 100

            df[f"dif_mensual_{prov}"] = total.diff()
            df[f"dif_inter_{prov}"] = total - total.shift(12)

        # Calcular variaciones y diferencias acumuladas (vs diciembre del año anterior)
        df["fecha"] = pd.to_datetime(df["fecha"])
        for anio in sorted(df["fecha"].dt.year.unique()):
            diciembre = df[(df["fecha"].dt.year == anio - 1) & (df["fecha"].dt.month == 12)]
            if not diciembre.empty:
                for prov in list(provincias.values()) + ["nea"]:
                    base = diciembre[f"total_{prov}"].values[0]
                    mask = df["fecha"].dt.year == anio
                    df.loc[mask, f"vacum_{prov}"] = ((df.loc[mask, f"total_{prov}"] / base) - 1) * 100
                    df.loc[mask, f"dif_acum_{prov}"] = df.loc[mask, f"total_{prov}"] - base

        # Guardar tabla
        df["fecha"] = df["fecha"].dt.date
        engine_dwh = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.dwh}")
        df.to_sql("empleo_nea_variaciones", engine_dwh, if_exists="replace", index=True)

