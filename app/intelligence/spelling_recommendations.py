from datetime import datetime

from app.database import get_connection


def generate_spelling_recommendations(user_id):

    conn = get_connection()
    cur = conn.cursor()

    # Step 0 — clear old recommendations for this user
    cur.execute(
        """
        DELETE FROM spelling_recommendations
        WHERE user_id = %s
        """,
        (user_id,),
    )

    # Step 1 — fetch stats
    cur.execute(
        """
        SELECT
            word_id,
            accuracy,
            attempts_count,
            wrong_count,
            last_attempt_at
        FROM spelling_word_stats
        WHERE user_id = %s
        """,
        (user_id,),
    )

    rows = cur.fetchall()

    recommendations = []
    now = datetime.utcnow()

    for row in rows:
        word_id, accuracy, attempts, wrong, last_attempt = row

        if attempts == 0:
            continue

        # recency factor (days since last attempt)
        if last_attempt:
            days_gap = (now - last_attempt).days
        else:
            days_gap = 10

        recency = min(days_gap / 10, 1)

        score = (
            (1 - accuracy) * 0.5 +
            (wrong / attempts) * 0.3 +
            recency * 0.2
        )

        reason = "weak word" if accuracy < 0.5 else "needs revision"

        recommendations.append((word_id, score, reason))

    # Step 2 — sort
    recommendations.sort(key=lambda x: x[1], reverse=True)

    # Step 3 — insert top 20
    for word_id, score, reason in recommendations[:20]:

        cur.execute(
            """
            INSERT INTO spelling_recommendations (
                user_id,
                word_id,
                recommendation_score,
                reason
            )
            VALUES (%s, %s, %s, %s)
            """,
            (user_id, word_id, score, reason),
        )

    conn.commit()
    cur.close()
    conn.close()

    return [
        {
            "word_id": w,
            "score": s,
            "reason": r,
        }
        for (w, s, r) in recommendations[:10]
    ]
