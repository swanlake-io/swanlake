import marimo

__generated_with = "0.18.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    from adbc_driver_flightsql.dbapi import connect

    conn = connect("grpc://127.0.0.1:4214")
    return (conn,)


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        SELECT 1;
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        use swanlake;
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        DROP TABLE IF EXISTS marimo_test;
        CREATE TABLE IF NOT EXISTS marimo_test (i INTEGER);
        INSERT INTO marimo_test SELECT 1 AS i UNION ALL SELECT 2;
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, marimo_test, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM marimo_test limit 10;
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        show tables;
        """,
        engine=conn
    )
    return


if __name__ == "__main__":
    app.run()
