import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import matplotlib.patheffects as pe

# ── Canvas ────────────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(26, 18))
ax.set_xlim(0, 26)
ax.set_ylim(0, 18)
ax.axis("off")
fig.patch.set_facecolor("#0d1117")
ax.set_facecolor("#0d1117")

# ── Palette ───────────────────────────────────────────────────────────────────
COLORS = {
    "data":       "#1a2a3a",
    "kafka":      "#1a2a1a",
    "spark":      "#2a1a1a",
    "airflow":    "#1a1a2a",
    "mlflow":     "#2a2a1a",
    "producer":   "#1a2a2a",
    "gateway":    "#2a1a2a",
    "frontend":   "#1a2a3a",
    "monitoring": "#2a1a1a",
    "storage":    "#1a2a20",
    "lane":       "#161b22",
}
BORDERS = {
    "data":       "#3a8fcd",
    "kafka":      "#3acd5a",
    "spark":      "#cd5a3a",
    "airflow":    "#8a5acd",
    "mlflow":     "#cdaa3a",
    "producer":   "#3acdcd",
    "gateway":    "#cd3aaa",
    "frontend":   "#5aabcd",
    "monitoring": "#cd6a3a",
    "storage":    "#3acd8a",
    "lane":       "#30363d",
}
TEXT_COLOR = "#e6edf3"
SUB_COLOR  = "#8b949e"
ARROW_COLOR = "#58a6ff"

# ── Helpers ───────────────────────────────────────────────────────────────────
def box(ax, x, y, w, h, title, tools, kind, alpha=0.95):
    """Draw a rounded component box with title + tool list."""
    fc = COLORS[kind]
    ec = BORDERS[kind]
    rect = FancyBboxPatch((x, y), w, h,
                          boxstyle="round,pad=0.07",
                          facecolor=fc, edgecolor=ec,
                          linewidth=1.8, alpha=alpha, zorder=3)
    ax.add_patch(rect)
    # colour bar at top
    bar = FancyBboxPatch((x, y + h - 0.32), w, 0.32,
                         boxstyle="round,pad=0.03",
                         facecolor=ec, edgecolor="none",
                         alpha=0.55, zorder=4)
    ax.add_patch(bar)
    ax.text(x + w / 2, y + h - 0.16, title,
            ha="center", va="center", fontsize=8.5, fontweight="bold",
            color="#ffffff", zorder=5, fontfamily="monospace")
    # tool list
    if tools:
        tool_str = "\n".join(f"• {t}" for t in tools)
        ax.text(x + w / 2, y + h / 2 - 0.18, tool_str,
                ha="center", va="center", fontsize=6.8,
                color=SUB_COLOR, zorder=5, linespacing=1.55,
                fontfamily="monospace")


def lane(ax, x, y, w, h, label):
    """Draw a swim-lane background."""
    rect = FancyBboxPatch((x, y), w, h,
                          boxstyle="round,pad=0.1",
                          facecolor=COLORS["lane"], edgecolor=BORDERS["lane"],
                          linewidth=1, alpha=0.6, zorder=1)
    ax.add_patch(rect)
    ax.text(x + 0.15, y + h / 2, label,
            ha="left", va="center", fontsize=7.5, fontweight="bold",
            color="#30363d", rotation=90, zorder=2, fontfamily="monospace")


def arrow(ax, x1, y1, x2, y2, label="", curved=False):
    style = "arc3,rad=0.25" if curved else "arc3,rad=0.0"
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-|>", color=ARROW_COLOR,
                                lw=1.3, connectionstyle=style),
                zorder=6)
    if label:
        mx, my = (x1 + x2) / 2, (y1 + y2) / 2
        ax.text(mx, my + 0.12, label, ha="center", va="bottom",
                fontsize=6, color=ARROW_COLOR, zorder=7,
                fontfamily="monospace")

# ── Swim Lanes ─────────────────────────────────────────────────────────────────
lane(ax, 0.3,  0.3,  3.2,  17.4, "DATA TIER")
lane(ax, 3.7,  0.3,  5.2,  17.4, "  MESSAGE BUS")
lane(ax, 9.1,  0.3,  5.2,  17.4, "  COMPUTE TIER")
lane(ax, 14.5, 0.3,  5.2,  17.4, " ORCHESTRATION")
lane(ax, 19.9, 0.3,  5.8,  17.4, "  FRONTEND / OBS")

# ── Title ─────────────────────────────────────────────────────────────────────
ax.text(13, 17.6, "Real-Time Fraud Detection — System Architecture",
        ha="center", va="center", fontsize=15, fontweight="bold",
        color=TEXT_COLOR, fontfamily="monospace",
        path_effects=[pe.withStroke(linewidth=3, foreground="#0d1117")])

# ══════════════════════════════════════════════════════════════════════════════
# DATA TIER  (x: 0.5 – 3.3)
# ══════════════════════════════════════════════════════════════════════════════
box(ax, 0.55, 13.5, 2.8, 3.5, "PostgreSQL",
    ["postgres:13", "Port: 5432", "Airflow Metadata", "MLflow Backend",
     "Fraud Records", "init_multiple_dbs.sh"], "data")

box(ax, 0.55, 9.5, 2.8, 3.5, "Redis (Broker)",
    ["redis:latest", "Port: 6379", "Celery Task Queue",
     "Airflow Worker Bus", "512 MB RAM"], "data")

box(ax, 0.55, 5.5, 2.8, 3.5, "Redis RAM Cache",
    ["redis:7-alpine", "Port: 6380", "allkeys-lru eviction",
     "4 GB tmpfs", "Gateway Cache Layer"], "gateway")

box(ax, 0.55, 1.2, 2.8, 3.8, "MinIO (S3)",
    ["minio/minio", "Ports: 9000/9001", "Model Artifacts",
     "Spark Checkpoints", "S3-compatible API",
     "Airflow S3 Conn"], "storage")

# ══════════════════════════════════════════════════════════════════════════════
# MESSAGE BUS — Kafka  (x: 3.9 – 8.7)
# ══════════════════════════════════════════════════════════════════════════════
box(ax, 3.9, 15.2, 4.8, 1.9, "Zookeeper",
    ["confluentinc/cp-zookeeper:7.3.0", "Port: 2181  |  512 MB"], "kafka")

box(ax, 3.9, 11.5, 4.8, 3.3, "Kafka Broker 1",
    ["cp-kafka:7.3.0", "Broker ID: 1", "Port: 9094 (SSL)",
     "INTERNAL: 9092", "SSL Keystore / Truststore",
     "Rep. Factor: 3  |  768 MB"], "kafka")

box(ax, 3.9, 7.8, 4.8, 3.3, "Kafka Broker 2",
    ["cp-kafka:7.3.0", "Broker ID: 2", "Port: 9095 (SSL)",
     "INTERNAL: 9092", "SSL Keystore / Truststore",
     "Rep. Factor: 3  |  768 MB"], "kafka")

box(ax, 3.9, 4.1, 4.8, 3.3, "Kafka Broker 3",
    ["cp-kafka:7.3.0", "Broker ID: 3", "Port: 9096 (SSL)",
     "INTERNAL: 9092", "SSL Keystore / Truststore",
     "Rep. Factor: 3  |  768 MB"], "kafka")

box(ax, 3.9, 1.2, 4.8, 2.5, "Transaction Producer",
    ["Python  |  Kafka-Python", "Topic: transactions",
     "GENERATOR mode", "REPLAY_MODE toggle",
     "512 MB  |  0.5 CPU"], "producer")

# ══════════════════════════════════════════════════════════════════════════════
# COMPUTE TIER — Spark  (x: 9.3 – 14.1)
# ══════════════════════════════════════════════════════════════════════════════
box(ax, 9.3, 14.5, 4.8, 2.7, "Spark Master",
    ["PySpark (Standalone)", "spark://spark-master:7077",
     "UI Port: 8085", "1 GB RAM  |  0.5 CPU"], "spark")

box(ax, 9.3, 11.0, 4.8, 3.0, "Spark Worker 1",
    ["Standalone Worker", "Cores: 2  |  1280 MB",
     "UI Port: 8083", "spark-checkpoints vol",
     "1.5 GB limit"], "spark")

box(ax, 9.3, 7.5, 4.8, 3.0, "Spark Worker 2",
    ["Standalone Worker", "Cores: 2  |  1280 MB",
     "UI Port: 8084", "spark-checkpoints vol",
     "1.5 GB limit"], "spark")

box(ax, 9.3, 1.2, 4.8, 5.9, "Spark Inference",
    ["Mode: LOCAL (Driver+Exec)", "streaming_job.py",
     "Structured Streaming", "Batch: 10s | Window: 1h",
     "Watermark: 10 min",
     "XGBoost model (pkl)", "SHADOW_MODE toggle",
     "MinIO: model artifacts", "tmpfs /tmp/spark: 4 GB",
     "3 GB RAM  |  2 CPU"], "spark")

# ══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATION — Airflow + MLflow  (x: 14.7 – 19.5)
# ══════════════════════════════════════════════════════════════════════════════
box(ax, 14.7, 14.0, 4.6, 3.2, "Airflow Webserver",
    ["apache/airflow", "Port: 8081", "UI / REST API",
     "CSRF Secret Key", "basic_auth backend",
     "1 GB  |  0.5 CPU"], "airflow")

box(ax, 14.7, 10.5, 4.6, 3.0, "Airflow Scheduler",
    ["Celery Executor", "DAG parsing & scheduling",
     "Triggers Celery tasks", "Redis as broker",
     "1 GB  |  0.5 CPU"], "airflow")

box(ax, 14.7, 7.0, 4.6, 3.0, "Airflow Worker",
    ["celery worker", "Executes DAG tasks",
     "Reads config.yaml", "Models → models_volume",
     "2 GB  |  1 CPU"], "airflow")

box(ax, 14.7, 4.5, 4.6, 2.0, "Airflow Triggerer",
    ["Deferrable operators",
     "Async task management"], "airflow")

box(ax, 14.7, 1.2, 4.6, 2.9, "MLflow Server",
    ["mlflow server", "Port: 5000", "Backend: PostgreSQL",
     "Artifacts: MinIO / local",
     "Optuna hyperparameters",
     "1 GB  |  0.5 CPU"], "mlflow")

# ══════════════════════════════════════════════════════════════════════════════
# FRONTEND + MONITORING  (x: 20.1 – 25.5)
# ══════════════════════════════════════════════════════════════════════════════
box(ax, 20.1, 14.0, 5.4, 3.5, "Secure Gateway",
    ["Python / FastAPI", "Encryption (Fernet)",
     "Kafka SSL Producer", "Redis Cache lookups",
     "TLS Keystores", "0.5 CPU  |  512 MB"], "gateway")

box(ax, 20.1, 10.0, 5.4, 3.5, "Web App (Dashboard)",
    ["Python Flask / app.py", "Port: 3000",
     "Live fraud metrics", "dashboard_ram (tmpfs)",
     "models_volume", "0.5 CPU  |  512 MB"], "frontend")

box(ax, 20.1, 6.8, 5.4, 2.8, "Prometheus",
    ["prom/prometheus:latest", "Port: 9090",
     "Scrapes: redis-exporter",
     "prometheus.yml config"], "monitoring")

box(ax, 20.1, 3.8, 5.4, 2.6, "Grafana",
    ["grafana/grafana:latest", "Port: 3001",
     "Admin PW: admin",
     "Provisioned dashboards",
     "Prometheus datasource"], "monitoring")

box(ax, 20.1, 1.2, 5.4, 2.2, "Redis Exporter",
    ["oliver006/redis_exporter",
     "Port: 9121",
     "Exposes Redis metrics",
     "0.2 CPU  |  128 MB"], "monitoring")

# ══════════════════════════════════════════════════════════════════════════════
# ARROWS — data flow
# ══════════════════════════════════════════════════════════════════════════════
# Producer → Kafka brokers
arrow(ax, 6.3, 2.5,  6.3, 4.1,  "transactions topic")
arrow(ax, 6.3, 5.4,  6.3, 7.8,  "")
arrow(ax, 6.3, 11.1, 6.3, 11.5, "")

# Zookeeper ↔ Kafka (coord)
arrow(ax, 6.3, 14.75, 6.3, 15.2, "coord")

# Kafka → Spark Inference (consume)
arrow(ax, 8.7, 5.8,  9.3, 3.5,  "consume (SSL)", curved=True)

# Spark Inference → MinIO (model read/write)
arrow(ax, 9.3, 2.5,  3.35, 2.5, "model r/w", curved=False)

# Spark → Redis RAM Cache (results)
arrow(ax, 9.3, 4.0,  3.35, 6.5, "predictions", curved=True)

# Redis Broker ↔ Airflow Scheduler
arrow(ax, 3.35, 10.5, 14.7, 11.0, "task queue", curved=False)

# Airflow Worker → PostgreSQL
arrow(ax, 14.7, 9.0, 3.35, 14.5, "metadata", curved=True)

# Airflow Worker → MLflow
arrow(ax, 14.7, 8.0, 14.7, 4.1, "log runs / params")

# MLflow → MinIO
arrow(ax, 14.7, 2.5, 3.35, 3.0, "artifacts", curved=True)

# Airflow Worker → Spark Master (submit)
arrow(ax, 14.7, 8.5, 14.1, 14.5, "job submit", curved=True)

# Spark Worker ← Spark Master
arrow(ax, 11.7, 14.5, 11.7, 14.0, "")
arrow(ax, 11.7, 14.5, 11.7, 10.5, "")

# Gateway ← Kafka
arrow(ax, 8.7, 8.8, 20.1, 15.0, "SSL produce", curved=True)

# Gateway → Redis Cache
arrow(ax, 20.1, 15.2, 3.35, 7.5, "cache write", curved=True)

# Web App ← Redis Cache
arrow(ax, 20.1, 11.5, 3.35, 7.0, "cache read", curved=True)

# Prometheus → Redis Exporter
arrow(ax, 22.8, 6.8, 22.8, 3.4, "scrape :9121")

# Grafana ← Prometheus
arrow(ax, 22.8, 3.8, 22.8, 6.8, "metrics", curved=False)

# Web App → Gateway
arrow(ax, 22.8, 10.0, 22.8, 10.0 + 4.0 - 0.5, "API calls", curved=False)

# ══════════════════════════════════════════════════════════════════════════════
# LEGEND
# ══════════════════════════════════════════════════════════════════════════════
legend_items = [
    (BORDERS["data"],       "Data Tier (PostgreSQL, Redis, MinIO)"),
    (BORDERS["kafka"],      "Message Bus (Kafka + Zookeeper)"),
    (BORDERS["spark"],      "Compute Tier (Spark Cluster)"),
    (BORDERS["airflow"],    "Orchestration (Airflow Celery)"),
    (BORDERS["mlflow"],     "ML Tracking (MLflow + Optuna)"),
    (BORDERS["gateway"],    "Gateway / Cache (Fernet + Redis)"),
    (BORDERS["frontend"],   "Frontend (Flask Dashboard)"),
    (BORDERS["monitoring"], "Monitoring (Prometheus + Grafana)"),
    (BORDERS["storage"],    "Object Storage (MinIO / S3)"),
    (ARROW_COLOR,           "Data Flow →"),
]
legend_patches = [mpatches.Patch(color=c, label=l) for c, l in legend_items]
ax.legend(handles=legend_patches, loc="lower center",
          ncol=5, fontsize=6.5, framealpha=0.25,
          facecolor="#161b22", edgecolor="#30363d",
          labelcolor=TEXT_COLOR,
          bbox_to_anchor=(0.5, -0.01))

plt.tight_layout()
plt.savefig("architecture_diagram.png", dpi=180, bbox_inches="tight",
            facecolor=fig.get_facecolor())
print("✅  Saved: architecture_diagram.png")
plt.show()
