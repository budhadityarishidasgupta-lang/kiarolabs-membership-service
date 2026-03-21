import requests

BASE_URL = "https://your-render-url.onrender.com"

EMAIL = "rishi@test.com"
PASSWORD = "test123"

def log(msg):
    print(f"\n=== {msg} ===")

def pass_log(msg):
    print(f"✅ {msg}")

def fail_log(msg, err):
    print(f"❌ {msg} -> {err}")

def test_login():
    log("LOGIN")
    try:
        res = requests.post(
            f"{BASE_URL}/login",
            data={"username": EMAIL, "password": PASSWORD},
        )
        if res.status_code == 200:
            token = res.json()["access_token"]
            pass_log("LOGIN")
            return token
        else:
            fail_log("LOGIN", res.text)
            return None
    except Exception as e:
        fail_log("LOGIN", str(e))
        return None

def test_courses(token):
    log("GET COURSES")
    try:
        res = requests.get(
            f"{BASE_URL}/practice/spelling/courses",
            headers={"Authorization": f"Bearer {token}"}
        )
        if res.status_code == 200:
            pass_log("GET COURSES")
            return res.json()
        else:
            fail_log("GET COURSES", res.text)
    except Exception as e:
        fail_log("GET COURSES", str(e))

def test_question(token, lesson_id):
    log("GET QUESTION")
    try:
        res = requests.get(
            f"{BASE_URL}/practice/spelling/question?lesson_id={lesson_id}",
            headers={"Authorization": f"Bearer {token}"}
        )
        if res.status_code == 200:
            pass_log("GET QUESTION")
            return res.json()
        else:
            fail_log("GET QUESTION", res.text)
    except Exception as e:
        fail_log("GET QUESTION", str(e))

def test_submit(token, word_id):
    log("SUBMIT ANSWER")
    try:
        res = requests.post(
            f"{BASE_URL}/practice/spelling/submit",
            json={
                "word_id": word_id,
                "answer": "test",
                "correct": False
            },
            headers={"Authorization": f"Bearer {token}"}
        )
        if res.status_code == 200:
            pass_log("SUBMIT ANSWER")
        else:
            fail_log("SUBMIT ANSWER", res.text)
    except Exception as e:
        fail_log("SUBMIT ANSWER", str(e))

def test_daily_goal(token):
    log("DAILY GOAL")
    try:
        res = requests.get(
            f"{BASE_URL}/progress/daily-goal",
            headers={"Authorization": f"Bearer {token}"}
        )
        if res.status_code == 200:
            pass_log("DAILY GOAL")
        else:
            fail_log("DAILY GOAL", res.text)
    except Exception as e:
        fail_log("DAILY GOAL", str(e))

def test_weekly(token):
    log("WEEKLY IMPROVEMENT")
    try:
        res = requests.get(
            f"{BASE_URL}/progress/weekly-improvement",
            headers={"Authorization": f"Bearer {token}"}
        )
        if res.status_code == 200:
            pass_log("WEEKLY IMPROVEMENT")
        else:
            fail_log("WEEKLY IMPROVEMENT", res.text)
    except Exception as e:
        fail_log("WEEKLY IMPROVEMENT", str(e))


def run_all():
    token = test_login()
    if not token:
        return

    courses = test_courses(token)

    # pick first lesson dynamically
    try:
        lesson_id = courses[0]["lessons"][0]["id"]
    except:
        print("⚠️ Could not extract lesson_id, using fallback 866")
        lesson_id = 866

    question = test_question(token, lesson_id)

    try:
        word_id = question["word_id"]
    except:
        print("⚠️ Could not extract word_id, using fallback 1")
        word_id = 1

    test_submit(token, word_id)
    test_daily_goal(token)
    test_weekly(token)

    print("\n🚀 TEST RUN COMPLETE")


if __name__ == "__main__":
    run_all()