"""
Connexion PostgreSQL via SQLAlchemy async (asyncpg).
Pool de connexions partagé sur toute la durée de vie de l'application.
"""

from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import NullPool
from app.core.config import settings

# Moteur async – NullPool recommandé derrière un proxy/PgBouncer
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DB_ECHO,
    pool_pre_ping=True,
    # NullPool évite les connexions zombies en environnement conteneurisé
    poolclass=NullPool,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency FastAPI – injecte une session par requête."""
    async with AsyncSessionLocal() as session:
        yield session
