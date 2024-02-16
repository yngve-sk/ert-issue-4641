from requests import Response


def test_get_summary_kw(poly_example_tmp_dir, dark_storage_client):
    resp: Response = dark_storage_client.get("/experiments")
    answer_json = resp.json()
    assert len(answer_json) == 1
    assert "ensemble_ids" in answer_json[0]
    assert len(answer_json[0]["ensemble_ids"]) == 2
    assert "name" in answer_json[0]
    assert answer_json[0]["name"] == "default"
