import main_2


def test_parse_usernames_supports_space_separated_profiles():
    assert main_2.parse_usernames("lucianaaa005 ashleyandria5") == [
        "lucianaaa005",
        "ashleyandria5",
    ]


def test_parse_usernames_supports_at_symbols_and_commas():
    assert main_2.parse_usernames("@alice, @bob charlie") == [
        "alice",
        "bob",
        "charlie",
    ]
