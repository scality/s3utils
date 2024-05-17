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

objectProcessingSpeed = TimeSeries(
    title="Rate of the S3 objects processed",
    dataSource="${DS_PROMETHEUS}",
    lineInterpolation="smooth",
    spanNulls=3*60*1000,
    unit="",
    targets=[
        Target(
            expr='rate(s3_countitems_total_objects_count{namespace="${namespace}", job=~"${job}"}[$__rate_interval])',
            legendFormat="{{status}}",
        ),
    ],
)

bucketProcessingSpeed = TimeSeries(
    title="Rate of the S3 buckets processed",
    dataSource="${DS_PROMETHEUS}",
    lineInterpolation="smooth",
    spanNulls=3*60*1000,
    unit="",
    targets=[
        Target(
            expr='rate(s3_countitems_total_buckets_count{namespace="${namespace}", job=~"${job}"}[$__rate_interval])',
            legendFormat="{{status}}",
        ),
    ],
)

consolidationDuration = Heatmap(
    title="Consolidation duration",
    dataSource="${DS_PROMETHEUS}",
    dataFormat="tsbuckets",
    maxDataPoints=25,
    tooltip=Tooltip(show=True, showHistogram=True),
    yAxis=YAxis(format=UNITS.SECONDS),
    color=HeatmapColor(mode="opacity"),
    targets=[Target(
        expr='sum by(le) (increase(s3_countitems_bucket_merge_duration_seconds_bucket{namespace="${namespace}", job="${job}"}[$__rate_interval]))',
        format="heatmap",
        legendFormat="{{le}}",
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
            RowPanel(title="Count items metrics"),
            layout.row([objectProcessingSpeed, consolidationDuration, bucketProcessingSpeed], height=8),
        ]),
    )
    .auto_panel_ids()
    .verify_datasources()
)


