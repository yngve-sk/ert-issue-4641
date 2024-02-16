from uuid import UUID

import xarray as xr
from fastapi import APIRouter, Body, Depends

from ert.dark_storage.enkf import get_storage
from ert.storage import StorageReader

router = APIRouter(tags=["ensemble"])
DEFAULT_STORAGE = Depends(get_storage)
DEFAULT_BODY = Body(...)


@router.get("/plotdata/{experiment_id}/{ensemble_ids}/{keyword}")
def get_summary_keyword(
    *,
    storage: StorageReader = DEFAULT_STORAGE,
    experiment_id: str,
    ensemble_ids: str,
    keyword: str
):
    ensemble_ids = ensemble_ids.split(",")
    selections = []

    for ens_id in ensemble_ids:
        ens = storage.get_ensemble(UUID(ens_id))
        assert ens.experiment_id == experiment_id
        selections.append(ens.load_responses_summary(keyword).sel(name=keyword))

    return (
        xr.combine_nested(selections, concat_dim="realization")
        .to_dataframe()
        .to_parquet()
    )
