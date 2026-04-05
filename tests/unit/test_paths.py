from app.domain.matching import local_to_manager_relative, manager_path_to_local


def test_manager_path_maps_to_local():
    mapped = manager_path_to_local(
        "/tv/Series/Season 01/Episode.mkv",
        [("/tv", "/data/media/tv")],
    )
    assert mapped is not None
    assert mapped.replace("\\", "/").endswith("/data/media/tv/Series/Season 01/Episode.mkv")


def test_local_path_maps_back_to_manager_relative():
    mapped = local_to_manager_relative(
        "/data/media/movies/Movie (2024)/Movie (2024).mkv",
        [("/movies", "/data/media/movies")],
    )
    assert mapped == ("/movies", "Movie (2024)/Movie (2024).mkv")
