import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium")


@app.cell(column=5)
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Overlagsanalyse mot hovedøkosystemkartet""")
    return


@app.cell(hide_code=True)
def _(arter_df, mo, pd):
    mo.stop(True, mo.md("⚠️ **Execution stopped**"))

    # Extract UTM coordinates from geometry column
    coords_utm = mo.sql("""
        SELECT 
            TRY_CAST(regexp_extract(geometry, 'POINT \\(([0-9.]+)', 1) AS DOUBLE) as x,
            TRY_CAST(regexp_extract(geometry, 'POINT \\([0-9.]+ ([0-9.]+)', 1) AS DOUBLE) as y
        FROM arter_df
        WHERE geometry IS NOT NULL 
            AND geometry != ''
            AND geometry LIKE 'POINT%'
    """)

    # Calculate bounding box with 10% buffer
    bbox_utm = mo.sql("""
        SELECT 
            MIN(x) - (MAX(x) - MIN(x)) * 0.1 as xmin,
            MAX(x) + (MAX(x) - MIN(x)) * 0.1 as xmax,
            MIN(y) - (MAX(y) - MIN(y)) * 0.1 as ymin,
            MAX(y) + (MAX(y) - MIN(y)) * 0.1 as ymax,
            (MAX(x) - MIN(x)) * (MAX(y) - MIN(y)) / 1000000 as area_km2
        FROM coords_utm
        WHERE x IS NOT NULL AND y IS NOT NULL
    """)

    # Convert to pandas DataFrame and extract values
    if hasattr(bbox_utm, "to_pandas"):
        bbox_df = bbox_utm.to_pandas()
    else:
        bbox_df = pd.DataFrame(bbox_utm)

    # Extract values from the DataFrame
    xmin = float(bbox_df["xmin"].values[0])
    xmax = float(bbox_df["xmax"].values[0])
    ymin = float(bbox_df["ymin"].values[0])
    ymax = float(bbox_df["ymax"].values[0])
    area_km2 = float(bbox_df["area_km2"].values[0])

    mo.md(f"""
    ### Bounding Box UTM Zone 33N (EPSG:25833)
    - **X Range:** {xmin:.0f} - {xmax:.0f}
    - **Y Range:** {ymin:.0f} - {ymax:.0f}
    - **Area:** {area_km2:.1f} km²
    """)
    return area_km2, coords_utm, xmax, xmin, ymax, ymin


@app.cell(hide_code=True)
def _(
    area_km2,
    go,
    map_style_dropdown,
    mo,
    pyproj,
    satellite_toggle,
    xmax,
    xmin,
    ymax,
    ymin,
):
    # Convert UTM bounding box corners to lat/lon for map display

    # Create transformer from UTM Zone 33N to WGS84
    transformer = pyproj.Transformer.from_crs("EPSG:25833", "EPSG:4326", always_xy=True)

    # Convert bounding box corners to lat/lon
    bbox_corners_utm = [
        (xmin, ymin),
        (xmax, ymin),
        (xmax, ymax),
        (xmin, ymax),
        (xmin, ymin),  # Close the polygon
    ]

    bbox_lons = []
    bbox_lats = []
    for x, y in bbox_corners_utm:
        lon, lat = transformer.transform(x, y)
        bbox_lons.append(lon)
        bbox_lats.append(lat)

    # Create plotly map figure
    fig_map_bbox = go.Figure()

    # Add the bounding box as a polygon on the map
    fig_map_bbox.add_trace(
        go.Scattermap(
            mode="lines",
            lon=bbox_lons,
            lat=bbox_lats,
            fill="toself",
            fillcolor="rgba(255, 0, 0, 0.2)",
            line=dict(width=3, color="red"),
            name="Bounding Box",
            text=f"Area: {area_km2:.1f} km²",
        )
    )

    # Calculate center of bounding box for map centering
    center_lon = sum(bbox_lons[:-1]) / 4
    center_lat = sum(bbox_lats[:-1]) / 4

    # Set zoom level - higher values zoom in more (typically 0-20)
    # zoom = 9  # City level
    # zoom = 12  # Neighborhood level
    # zoom = 15  # Street level
    zoom_level = 7

    # Update layout with map settings
    fig_map_bbox.update_layout(
        map=dict(
            style=map_style_dropdown.value,
            center=dict(lat=center_lat, lon=center_lon),
            zoom=zoom_level,
        ),
        height=700,
        title="UTM Zone 33N Bounding Box on Map",
        showlegend=True,
    )

    # Add satellite imagery if toggle is on
    if satellite_toggle.value:
        fig_map_bbox.update_layout(
            map_style="white-bg",
            map_layers=[
                {
                    "below": "traces",
                    "sourcetype": "raster",
                    "source": [
                        "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
                    ],
                }
            ],
        )

    mo.ui.plotly(fig_map_bbox)
    return


@app.cell(hide_code=True)
def _():
    service_url = "https://kart2.miljodirektoratet.no/arcgis/rest/services/hovedokosystem/hovedokosystem/MapServer"
    return (service_url,)


@app.cell(hide_code=True)
def _(mo, requests, service_url, time, xmax, xmin, ymax, ymin):
    from typing import Any

    # If envelope works better, here's an updated download function
    def download_arcgis_utm33_envelope(
        service_url: str,
        layer_id: int,
        xmin: float,
        ymin: float,
        xmax: float,
        ymax: float,
        max_records: int = 2000,
    ) -> dict[str, Any]:
        """Download GeoJSON data using envelope geometry.

        Queries an ArcGIS MapServer layer within a UTM 33N bounding box,
        handling pagination to retrieve all matching features.

        Args:
            service_url: Base URL of the ArcGIS MapServer service.
            layer_id: Numeric layer index within the service.
            xmin: Left edge of the bounding box (EPSG:25833).
            ymin: Bottom edge of the bounding box (EPSG:25833).
            xmax: Right edge of the bounding box (EPSG:25833).
            ymax: Top edge of the bounding box (EPSG:25833).
            max_records: Maximum features per request page. Defaults to 2000.

        Returns:
            A GeoJSON FeatureCollection dict with ``type`` and ``features`` keys.
        """
        base_url = f"{service_url}/{layer_id}/query"

        # Use envelope format for the geometry
        base_params = {
            "geometry": f"{xmin},{ymin},{xmax},{ymax}",
            "geometryType": "esriGeometryEnvelope",
            "spatialRel": "esriSpatialRelIntersects",
            "inSR": "25833",
            "outSR": "25833",
            "where": "1=1",
            "f": "json",
        }

        # Get total count first
        count_params = {**base_params, "returnCountOnly": "true"}

        try:
            response = requests.get(base_url, params=count_params)
            response.raise_for_status()
            result = response.json()

            if "error" in result:
                mo.md(f"**Service error:** {result['error']}")
                return {"type": "FeatureCollection", "features": []}

            total_count = result.get("count", 0)
            mo.md(f"**Found {total_count} features in the bounding box**")

            if total_count == 0:
                return {"type": "FeatureCollection", "features": []}

        except requests.exceptions.RequestException as e:
            mo.md(f"**Error querying service:** {str(e)}")
            return {"type": "FeatureCollection", "features": []}

        # Download features with pagination
        all_features = []
        offset = 0

        while offset < total_count:
            query_params = {
                **base_params,
                "outFields": "*",
                "returnGeometry": "true",
                "resultOffset": offset,
                "resultRecordCount": min(max_records, total_count - offset),
                "f": "geojson",
            }

            try:
                response = requests.get(base_url, params=query_params)
                response.raise_for_status()

                geojson_data = response.json()
                features = geojson_data.get("features", [])
                all_features.extend(features)

                downloaded = len(features)
                offset += downloaded

                mo.md(f"Progress: {offset}/{total_count} features downloaded")

                if downloaded == 0:
                    break

                time.sleep(0.1)

            except requests.exceptions.RequestException as e:
                mo.md(f"**Error downloading batch at offset {offset}:** {str(e)}")
                break

        return {"type": "FeatureCollection", "features": all_features}

    # Try the envelope-based download
    ecosystem_geojson_envelope = download_arcgis_utm33_envelope(
        service_url, 0, xmin, ymin, xmax, ymax
    )

    mo.md(f"""### Lastet ned data fra økologisk grunnkart
    Downloaded **{len(ecosystem_geojson_envelope["features"])}** ecosystem polygons
    """)
    return (ecosystem_geojson_envelope,)


@app.cell(hide_code=True)
def _(ecosystem_geojson_envelope, json, tempfile):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False) as f:
        json.dump(ecosystem_geojson_envelope, f)
        temp_geojson_path = f.name
    return (temp_geojson_path,)


@app.cell(hide_code=True)
def _(os):
    os.environ["OGR_GEOJSON_MAX_OBJ_SIZE"] = "0"
    return


@app.cell(hide_code=True)
def _(mo, temp_geojson_path):
    _df = mo.sql(
        f"""
        INSTALL spatial;
        LOAD spatial;

        CREATE OR REPLACE TABLE ecosystems AS
        SELECT * FROM ST_Read('{temp_geojson_path}');
        """
    )
    return (ecosystems,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Husk at du filtrerer på økosystemtyper ved å velge nummer 1-12 eller flere ved 1,2,5. 
    - Endre denne    : "WHERE ecotype IN (4)"
    """
    )
    return


@app.cell(hide_code=True)
def _(arter_df, ecosystems, mo):
    system_arter_df = mo.sql(
        f"""
        WITH species_points AS (
            -- Use the existing geometry column (already in UTM 33N)
            SELECT 
                *,
                ST_GeomFromText(geometry) AS geom
            FROM arter_df
            WHERE geometry IS NOT NULL 
              AND geometry != ''
              AND geometry LIKE 'POINT%'
        ),
        filtered_ecosystems AS (
            -- Pre-filter ecosystems to specific types
            SELECT 
                ecotype,
                geom AS polygon_geom
            FROM ecosystems
            WHERE ecotype IN (4)
        )
        -- Optimized spatial join using SPATIAL_JOIN operator
        SELECT 
            sp.* EXCLUDE (geom, geometry),
            fe.ecotype AS ecosystem_type
        FROM species_points sp
        INNER JOIN filtered_ecosystems fe
            ON ST_Intersects(sp.geom, fe.polygon_geom)
        """,
        output=False,
    )
    return (system_arter_df,)


@app.cell
def _(mo, system_arter_df):
    okosystem_arter_df = mo.ui.table(system_arter_df)
    okosystem_arter_df
    return


if __name__ == "__main__":
    app.run()
