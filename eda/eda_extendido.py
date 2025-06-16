

from pathlib import Path
import warnings, numpy as np, pandas as pd, matplotlib.pyplot as plt
from sqlalchemy import text
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
from scipy.stats import norm, kurtosis
from eda_connection import get_engine   # misma conexión que ETL

try:
    import seaborn as sns
    sns.set_theme(style="whitegrid")
except ImportError:
    warnings.warn("Seaborn no instalado; boxplots usarán Matplotlib.")


EDA_DIR    = Path(__file__).resolve().parent
REPORT_DIR = EDA_DIR / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)


engine = get_engine()
with engine.connect() as conn:
    conn.execute(text("SELECT 1"))
print("✅ Conexión OK — extrayendo tablas…")

studentInfo        = pd.read_sql("studentInfo",        engine)
studentAssessment  = pd.read_sql("studentAssessment",  engine)
studentVle         = pd.read_sql("SELECT TOP 2000000 * FROM studentVle", engine)


summary = pd.DataFrame({
    "table": ["studentInfo", "studentAssessment", "studentVle (2M)"],
    "rows":  [len(studentInfo), len(studentAssessment), len(studentVle)],
    "cols":  [studentInfo.shape[1], studentAssessment.shape[1], studentVle.shape[1]]
})
summary.to_csv(REPORT_DIR / "00_tabla_resumen.csv", index=False)
summary.to_html(REPORT_DIR / "00_tabla_resumen.html", index=False)

#Bar plot resultados finales
plt.figure(figsize=(6,4))
studentInfo["final_result"].value_counts().plot(kind="bar")
plt.title("Distribución de resultados finales")
plt.tight_layout()
plt.savefig(REPORT_DIR / "bar_resultados.png", dpi=300)
plt.close()

#Boxplot score vs resultado
df_scores = (
    studentAssessment.groupby("id_student")["score"].mean().rename("avg_score")
    .to_frame()
    .merge(studentInfo[["id_student", "final_result"]], on="id_student")
    .dropna()
)
plt.figure(figsize=(8,5))
if "sns" in globals():
    sns.boxplot(x="final_result", y="avg_score", data=df_scores)
else:
    plt.boxplot([g["avg_score"].values for _, g in df_scores.groupby("final_result")],
                labels=df_scores["final_result"].unique())
plt.title("Score promedio vs Resultado final")
plt.tight_layout()
plt.savefig(REPORT_DIR / "boxplot_score_vs_result.png", dpi=300)
plt.close()

#Heatmap correlación
num_cols = studentInfo.select_dtypes(include="number")
plt.figure(figsize=(10,8))
plt.imshow(num_cols.corr(), cmap="viridis", interpolation="nearest")
plt.xticks(range(len(num_cols.columns)), num_cols.columns, rotation=90)
plt.yticks(range(len(num_cols.columns)), num_cols.columns)
plt.title("Matriz de correlación — studentInfo")
plt.colorbar()
plt.tight_layout()
plt.savefig(REPORT_DIR / "heatmap_correlacion.png", dpi=300)
plt.close()

#Dispersión créditos vs score
df_disp = (
    studentInfo[["id_student", "studied_credits"]]
    .merge(df_scores[["id_student", "avg_score"]], on="id_student")
)
plt.figure(figsize=(6,5))
plt.scatter(df_disp["studied_credits"], df_disp["avg_score"], s=8, alpha=.3)
plt.title("Dispersión: créditos vs score promedio")
plt.xlabel("Studied credits"); plt.ylabel("Average score")
plt.tight_layout()
plt.savefig(REPORT_DIR / "dispersion_credits_vs_score.png", dpi=300)
plt.close()

#Campana de Gauss sobre Score
scores = studentAssessment["score"].dropna()
mu, sigma = scores.mean(), scores.std()
plt.figure(figsize=(6,4))
plt.hist(scores, bins=40, density=True, alpha=.6)
x = np.linspace(scores.min(), scores.max(), 200)
plt.plot(x, norm.pdf(x, mu, sigma), linewidth=2)
plt.title("Campana de Gauss — Score")
plt.tight_layout()
plt.savefig(REPORT_DIR / "gauss_score.png", dpi=300)
plt.close()

#Kurtosis
k_vals = kurtosis(num_cols, nan_policy="omit")
plt.figure(figsize=(8,4))
plt.bar(num_cols.columns, k_vals)
plt.xticks(rotation=45, ha="right")
plt.title("Kurtosis variables numéricas")
plt.tight_layout()
plt.savefig(REPORT_DIR / "kurtosis.png", dpi=300)
plt.close()

#Matriz de confusión (score ≥ 40)
df_cf = df_scores.merge(
    studentInfo[["id_student", "final_result"]],
    on="id_student",
    suffixes=("", "_info")
)
if "final_result_info" in df_cf.columns:
    df_cf = df_cf.drop(columns=["final_result"]).rename(
        columns={"final_result_info": "final_result"}
    )

df_cf["pred"]   = np.where(df_cf["avg_score"] >= 40, 1, 0)
df_cf["actual"] = np.where(df_cf["final_result"].isin(["Pass","Distinction"]), 1, 0)
cm = confusion_matrix(df_cf["actual"], df_cf["pred"])
ConfusionMatrixDisplay(cm, display_labels=["Fail","Pass"]).plot(colorbar=False)
plt.title("Matriz de confusión (score ≥ 40)")
plt.tight_layout()
plt.savefig(REPORT_DIR / "confusion_matrix.png", dpi=300)
plt.close()

#Histograma sum_click
plt.figure(figsize=(8,4))
studentVle["sum_click"].plot.hist(bins=50)
plt.title("Histograma sum_click (muestra 2M)")
plt.xlabel("sum_click")
plt.tight_layout()
plt.savefig(REPORT_DIR / "hist_sum_click.png", dpi=300)
plt.close()

print(f"  EDA completado — archivos en {REPORT_DIR}")
