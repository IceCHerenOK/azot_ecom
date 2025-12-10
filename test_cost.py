from gs_client import get_cost_by_article

if __name__ == "__main__":
    test_art = "DBTACHO 1.0"  # подставь любой ісходящийся с таблицей артикул
    cost = get_cost_by_article(test_art)
    print(f"Артикул: {test_art}")
    print(f"Себестоимость: {cost}")
