from grafanalib.core import (
    ConstantInput,
    DataSourceInput,
    Heatmap,
    HeatmapColor,
    HIDE_VARIABLE,
    RowPanel,
    Stat,
    Template,
    Templating,
    Threshold,
    YAxis,
)
from grafanalib import formatunits as UNITS
from scalgrafanalib import (
    layout,
    BarGauge,
    Dashboard,
    GaugePanel,
    PieChart,
    Tooltip,
    Target,
    TimeSeries
)

bucketCountDuration = TimeSeries(
    title="bucket count duration",
    dataSource="${DS_PROMETHEUS}",
    lineInterpolation="smooth",
    spanNulls=3*60*1000,
    unit=UNITS.SECONDS,
    targets=[Target(
        expr='sum(rate(count_items_bucketProcessingDuration_count{namespace="${namespace}", job=~"${job}"}[$__rate_interval])))',
        legendFormat='{{namespace}} - {{job}}'
    )],
)

consolidationDuration = TimeSeries(
    title="consolidation duration",
    dataSource="${DS_PROMETHEUS}",
    lineInterpolation="smooth",
    spanNulls=3*60*1000,
    unit=UNITS.SECONDS,
    targets=[Target(
        expr='sum(rate(count_items_consolidationDuration_count{namespace="${namespace}", job=~"${job}"}[$__rate_interval]))',
        legendFormat='{{namespace}} - {{job}}'
    )],
)


dashboard = (
    Dashboard(
        title="S3Utils service",
        editable=True,
        refresh="30s",
        tags=["S3Utils"],
        timezone="",
        inputs=[
            DataSourceInput(
                name="DS_PROMETHEUS",
                label="Prometheus",
                pluginId="prometheus",
                pluginName="Prometheus",
            ),
            DataSourceInput(
                name="DS_LOKI",
                label="Loki",
                pluginId="loki",
                pluginName="Loki"
            ),
            ConstantInput(
                name="namespace",
                label="namespace",
                description="Namespace associated with the Zenko instance",
                value="zenko",
            ),
            ConstantInput(
                name="job",
                label="job",
                description="Name of the S3utils job, used to filter the "
                            "metrics.",
                value="artesca-data-ops-count-items-metrics",
            ),
            ConstantInput(
                name="pod",
                label="pod",
                description="Prefix of the cronjob pod name, used to filter "
                            "only the cronjob instances.",
                value="artesca-data-ops-count-items",
            ),
        ],
        panels=layout.column([
            RowPanel(title="Processing Duration"),
            layout.row([bucketCountDuration], height=8),
        ]),
    )
    .auto_panel_ids()
    .verify_datasources()
)


