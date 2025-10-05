from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

class Base(DeclarativeBase):
    pass

class Product(Base):
    __tablename__ = "products"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    category: Mapped[str]
    min_price: Mapped[int]
    max_price: Mapped[int]
    rating: Mapped[int]
    reviews_count: Mapped[int]

    def __repr__(self) -> str:
        return f"Product(id={self.id!r}, name={self.name!r}, category={self.category!r}, min_price={self.min_price!r}, max_price={self.max_price!r}, rating={self.rating!r}, reviews_count={self.reviews_count!r})"

class Offer(Base):
    __tablename__ = "offers"
    id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))
    seller: Mapped[str]
    price: Mapped[int]

    def __repr__(self) -> str:
        return f"Offer(id={self.id!r}, product_id={self.product_id!r}, seller={self.seller!r}, price={self.price!r})"

