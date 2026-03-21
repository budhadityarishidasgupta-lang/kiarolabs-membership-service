import requests

BASE_URL = "https://kiarolabs-membership-service.onrender.com"

EMAIL = "rishi@test.com"
PASSWORD = "test123"

def check(name, res):
    if res.status_code == 200:
        print(f"✅ {name}")
    else:
        print(f"❌ {name} -> {res.text}")

def login():
    print("\n=== LOGIN ===")
    res = requests.post(
        f"{BASE_URL}/login",
        data={"username": EMAIL, "password": PASSWORD},
    )
    check("LOGIN", res)
    return res.json().get("access_token") if res.status_code == 200 else None

def test_spelling(token):
    headers = {"Authorization": f"Bearer {token}"}

    print("\n=== SPELLING TEST ===")

    res = requests.get(f"{BASE_URL}/practice/spelling/courses", headers=headers)
    check("SPELLING COURSES", res)

    res = requests.get(f"{BASE_URL}/practice/spelling/question?lesson_id=866", headers=headers)
    check("SPELLING QUESTION", res)

def test_words(token):
    headers = {"Authorization": f"Bearer {token}"}

    print("\n=== WORDSPRINT TEST ===")

    res = requests.get(f"{BASE_URL}/practice/words/courses", headers=headers)
    check("WORDS COURSES", res)

    res = requests.get(f"{BASE_URL}/practice/words/question?lesson_id=1", headers=headers)
    check("WORDS QUESTION", res)

def run():
    token = login()
    if not token:
        return

    test_spelling(token)
    test_words(token)

    print("\n🚀 TEST COMPLETE")

if __name__ == "__main__":
    run()
