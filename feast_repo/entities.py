"""
Feast Entity Definitions for ToGather ML Platform.

Entities are the primary keys used to look up features.
"""

from feast import Entity

# User entity - for user-level features
user = Entity(
    name="user",
    join_keys=["user_id"],
    description="ToGather platform user",
)

# Experience entity - for experience/event-level features
experience = Entity(
    name="experience",
    join_keys=["experience_id"],
    description="ToGather experience or event listing",
)

# Session entity - for session-level features
session = Entity(
    name="session",
    join_keys=["session_id"],
    description="User browsing session",
)
