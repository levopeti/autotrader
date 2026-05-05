import pandas as pd

# CSV-k betöltése
source = pd.read_csv("/home/ad.adasworks.com/levente.peto/Downloads/leveraged_trades_history_24.03.2026.csv")   # az amelyikből vesszük az rpl értéket
target = pd.read_csv("./positions.csv")   # az amelyiket frissítjük

# Lookup map: Order Id → rpl
rpl_map = source.set_index("Order Id")["rpl"].to_dict()

# realised_pnl frissítése deal_id alapján
# Ha csak a hiányzó értékeket akarod kitölteni:
target["realised_pnl"] = target.apply(
    lambda row: rpl_map.get(row["deal_id"], row["realised_pnl"])
    if pd.isna(row["realised_pnl"]) or str(row["realised_pnl"]).strip() == ""
    else row["realised_pnl"],
    axis=1
)

# Ha minden sort felül akarsz írni (nem csak az üreseket), cseréld az előző részt:
# target["realised_pnl"] = target["deal_id"].map(rpl_map).fillna(target["realised_pnl"])

# Mentés
target.to_csv("./positions.csv", index=False)

print(f"Kész. {target['realised_pnl'].notna().sum()} sor tartalmaz realised_pnl értéket.")