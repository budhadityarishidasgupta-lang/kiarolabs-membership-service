import requests

BASE_URL = "https://your-render-url.onrender.com"

EMAIL = "rishi@test.com"
PASSWORD = "test123"

def check(name, condition, error=""):
    if condition:
        print(f"✅ {name}")
    else:
        print(f"❌ {name} -> {error}")

def login():
    res = requests.post(
        f"{BASE_URL}/login",
        data={"username": EMAIL, "password": PASSWORD},
    )
    check("LOGIN", res.status_code == 200, res.text)
    return res.json().get("access_token") if res.status_code == 200 else None

def test_spelling(token):
    headers = {"Authorization": f"Bearer {token}"}

    res = requests.get(f"{BASE_URL}/practice/spelling/courses", headers=headers)
    check("SPELLING COURSES", res.status_code == 200, res.text)

    res = requests.get(f"{BASE_URL}/practice/spelling/question?lesson_id=866", headers=headers)
    check("SPELLING QUESTION", res.status_code == 200, res.text)

def test_words(token):
    headers = {"Authorization": f"Bearer {token}"}

    res = requests.get(f"{BASE_URL}/practice/words/courses", headers=headers)
    check("WORDS COURSES", res.status_code == 200, res.text)

    res = requests.get(f"{BASE_URL}/practice/words/question?lesson_id=1", headers=headers)
    check("WORDS QUESTION", res.status_code == 200, res.text)

def run():
    token = login()
    if not token:
        return

    test_spelling(token)
    test_words(token)

    print("\n🚀 TEST COMPLETE")

if __name__ == "__main__":
    run()
