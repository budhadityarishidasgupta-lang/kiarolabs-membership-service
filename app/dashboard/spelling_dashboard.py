from app.database import get_connection


def get_spelling_dashboard(user_id: int):
    conn = get_connection()
    cur = conn.cursor()

    # 1. Overall stats
    cur.execute(
        """
        SELECT
            COUNT(*) AS total_words,
            COALESCE(AVG(accuracy), 0) AS avg_accuracy,
            SUM(attempts_count) AS total_attempts
        FROM spelling_word_stats
        WHERE user_id = %s
    """,
        (user_id,),
    )

    overall = cur.fetchone()

    # 2. Weak words (accuracy < 0.5)
    cur.execute(
        """
        SELECT word_id, accuracy
        FROM spelling_word_stats
        WHERE user_id = %s
        AND accuracy < 0.5
        ORDER BY accuracy ASC
        LIMIT 5
    """,
        (user_id,),
    )

    weak_words = cur.fetchall()

    # 3. Recommended words
    cur.execute(
        """
        SELECT word_id, recommendation_score, reason
        FROM spelling_recommendations
        WHERE user_id = %s
        ORDER BY recommendation_score DESC
        LIMIT 5
    """,
        (user_id,),
    )

    recommendations = cur.fetchall()

    cur.close()
    conn.close()

    return {
        "summary": {
            "total_words": overall[0] or 0,
            "avg_accuracy": float(overall[1] or 0),
            "total_attempts": overall[2] or 0,
        },
        "weak_words": [{"word_id": w, "accuracy": a} for (w, a) in weak_words],
        "recommendations": [
            {"word_id": w, "score": s, "reason": r} for (w, s, r) in recommendations
        ],
    }
