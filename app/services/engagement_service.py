from datetime import date, timedelta

from app.database import get_connection


def update_user_engagement(user_id: int, xp_earned: int):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT total_xp, current_streak, last_activity_date
        FROM user_engagement
        WHERE user_id = %s
        """,
        (user_id,),
    )

    row = cur.fetchone()
    today = date.today()

    if not row:
        cur.execute(
            """
            INSERT INTO user_engagement (user_id, total_xp, current_streak, last_activity_date)
            VALUES (%s, %s, %s, %s)
            """,
            (user_id, xp_earned, 1, today),
        )

        conn.commit()
        cur.close()
        conn.close()

        return {"xp": xp_earned, "streak": 1}

    total_xp, current_streak, last_activity = row

    if last_activity == today:
        new_streak = current_streak
    elif last_activity == today - timedelta(days=1):
        new_streak = current_streak + 1
    else:
        new_streak = 1

    new_total_xp = total_xp + xp_earned

    cur.execute(
        """
        UPDATE user_engagement
        SET total_xp = %s,
            current_streak = %s,
            last_activity_date = %s,
            updated_at = NOW()
        WHERE user_id = %s
        """,
        (new_total_xp, new_streak, today, user_id),
    )

    conn.commit()
    cur.close()
    conn.close()

    return {
        "xp": new_total_xp,
        "streak": new_streak,
    }
