"""Data package. Import DataProvider explicitly to avoid eager optional network dependency."""
from .storage_manager import StorageManager

__all__ = ["StorageManager"]
