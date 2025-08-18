from sqlalchemy.engine.create import create_engine
from sqlalchemy.sql.expression import text    

if __name__ == "__main__":    
    # use password part as the API key
    engine = create_engine("duckdb_http://localhost:9999?api_key=secretkey")
    with engine.connect() as conn:     # type: ignore
        result = conn.execute(text("SELECT * from transactions where block_time = 1746089244"))
        for row in result:
            print(row)

        