from sqlalchemy import create_engine,engine

def get_mysql_financialdata_conn() -> engine.base.Connection:
    """    
    user: root
    password: 123456
    host: localhost
    port: 3306
    database: financialdata
    如果有實體 IP，以上設定可以自行更改

    Returns:
        engine.base.Connection: _description_
    """
    address = "mysql+pymysql://root:123456@localhost:3306/financialdata"
    engine = create_engine(address)
    connect = engine.connect()
    return connect


