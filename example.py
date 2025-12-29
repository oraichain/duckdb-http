from sqlalchemy.engine.create import create_engine
from sqlalchemy.orm import Session
from sqlalchemy import String, Float, BigInteger, Column, TIMESTAMP
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Transaction(Base):
    __tablename__ = "transactions"

    signature = Column(
        String, primary_key=True, nullable=False
    )  # assuming signature is unique, else add an id column
    block_time = Column(BigInteger, nullable=True)
    human_time = Column(TIMESTAMP, nullable=True)
    action = Column(String, nullable=True)
    from_address = Column(
        "from", String, nullable=True
    )  # "from" is a Python keyword, so renamed attribute
    token1 = Column(String, nullable=True)
    amount1 = Column(BigInteger, nullable=True)
    tokendecimals1 = Column(BigInteger, nullable=True)
    token2 = Column(String, nullable=True)
    amount2 = Column(BigInteger, nullable=True)
    tokendecimals2 = Column(BigInteger, nullable=True)
    value = Column(Float, nullable=True)
    platforms = Column(String, nullable=True)
    sources = Column(String, nullable=True)

    def __repr__(self):
        return f"<Transaction(signature={self.signature}, block_time={self.block_time}, action={self.action})>"


if __name__ == "__main__":
    # use password part as the API key
    engine = create_engine("duckdb_http://localhost:9999?api_key=mysecret")
    with Session(engine) as session:  # type: ignore
        result = session.query(Transaction).limit(10).all()
        for row in result:
            print(row)
