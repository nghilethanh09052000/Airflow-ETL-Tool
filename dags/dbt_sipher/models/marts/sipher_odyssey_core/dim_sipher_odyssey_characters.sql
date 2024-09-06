{{- config(
  materialized='table',
) -}}

SELECT
  'INU.Normal' AS id,
  'Canis' AS sub_race,
  '0' AS ranking,
  'INU' AS race,
  'INU_Canis' AS character, 
  'AssaultRifle_Default_ID1' AS gun,
  'Katana_Default_ID4' AS melee,
  'NonNFT_Exclusive_Inu_Accessories_InuSuit_ID1' AS accessories,
  'NonNFT_Exclusive_Inu_Body_InuSuit_ID1' AS body,
  'NonNFT_Exclusive_Inu_Hands_InuSuit_ID1' AS hands,
  'NonNFT_Exclusive_Inu_Legs_InuSuit_ID1' AS legs,
  'NonNFT_Exclusive_Inu_Feet_InuSuit_ID1' AS feet,
  'SipherCharacterSubraceDataAsset:SDA_INU_SubraceNormal' AS asset_id
UNION ALL
SELECT
'INU.Cyborg' AS id,
'Cyborg' AS sub_race,
'2' AS ranking,
'INU' AS race,
'INU_Cyborg' AS character, 
'AssaultRifle_Default_ID1' AS gun,
'Katana_Default_ID4' AS melee,
'NonNFT_Exclusive_Inu_Accessories_InuSuit_ID1' AS accessories,
'NonNFT_Exclusive_Inu_Body_InuSuit_ID1' AS body,
'NonNFT_Exclusive_Inu_Hands_InuSuit_ID1' AS hands,
'NonNFT_Exclusive_Inu_Legs_InuSuit_ID1' AS legs,
'NonNFT_Exclusive_Inu_Feet_InuSuit_ID1' AS feet,
'SipherCharacterSubraceDataAsset:SDA_INU_SubraceCyborg' AS asset_id

UNION ALL
SELECT
'INU.Elemental' AS id,
'BioZ' AS sub_race,
'1' AS ranking,
'INU' AS race,
'INU_Bioz' AS character, 
'AssaultRifle_Default_ID1' AS gun,
'Katana_Default_ID4' AS melee,
'NonNFT_Exclusive_Inu_Accessories_InuSuit_ID1' AS accessories,
'NonNFT_Exclusive_Inu_Body_InuSuit_ID1' AS body,
'NonNFT_Exclusive_Inu_Hands_InuSuit_ID1' AS hands,
'NonNFT_Exclusive_Inu_Legs_InuSuit_ID1' AS legs,
'NonNFT_Exclusive_Inu_Feet_InuSuit_ID1' AS feet,
'SipherCharacterSubraceDataAsset:SDA_INU_SubraceElemental' AS asset_id

UNION ALL
SELECT
'INU.Supernatural' AS id,
'Cosmic' AS sub_race,
'3' AS ranking,
'INU' AS race,
'INU_Cosmic' AS character, 
'AssaultRifle_Default_ID1' AS gun,
'Katana_Default_ID4' AS melee,
'NonNFT_Exclusive_Inu_Accessories_InuSuit_ID1' AS accessories,
'NonNFT_Exclusive_Inu_Body_InuSuit_ID1' AS body,
'NonNFT_Exclusive_Inu_Hands_InuSuit_ID1' AS hands,
'NonNFT_Exclusive_Inu_Legs_InuSuit_ID1' AS legs,
'NonNFT_Exclusive_Inu_Feet_InuSuit_ID1' AS feet,
'SipherCharacterSubraceDataAsset:SDA_INU_SubraceSupernatural' AS asset_id

UNION ALL
SELECT
'NEKO.Normal' AS id,
'Felis' AS sub_race,
'0' AS ranking,
'NEKO' AS race,
'NEKO_Felis' AS character, 
'DualSMG_Default_ID2' AS gun,
'LightningWhip_Default_ID5' AS melee,
'NonNFT_Exclusive_NEKO_Accessories_NekoSuit_ID2' AS accessories,
'NonNFT_Exclusive_NEKO_Body_NekoSuit_ID2' AS body,
'NonNFT_Exclusive_NEKO_Hands_NekoSuit_ID2' AS hands,
'NonNFT_Exclusive_NEKO_Legs_NekoSuit_ID2' AS legs,
'NonNFT_Exclusive_NEKO_Feet_NekoSuit_ID2' AS feet,
'SipherCharacterSubraceDataAsset:SDA_NEKO_SubraceNormal' AS asset_id

UNION ALL
SELECT
'NEKO.Cyborg' AS id,
'Synthetic' AS sub_race,
'1' AS ranking,
'NEKO' AS race,
'NEKO_Synthetic' AS character, 
'DualSMG_Default_ID2' AS gun,
'LightningWhip_Default_ID5' AS melee,
'NonNFT_Exclusive_NEKO_Accessories_NekoSuit_ID2' AS accessories,
'NonNFT_Exclusive_NEKO_Body_NekoSuit_ID2' AS body,
'NonNFT_Exclusive_NEKO_Hands_NekoSuit_ID2' AS hands,
'NonNFT_Exclusive_NEKO_Legs_NekoSuit_ID2' AS legs,
'NonNFT_Exclusive_NEKO_Feet_NekoSuit_ID2' AS feet,
'SipherCharacterSubraceDataAsset:SDA_NEKO_SubraceCyborg' AS asset_id

UNION ALL
SELECT
'NEKO.Elemental' AS id,
'Crystalis' AS sub_race,
'3' AS ranking,
'NEKO' AS race,
'NEKO_Crystalis' AS character, 
'DualSMG_Default_ID2' AS gun,
'LightningWhip_Default_ID5' AS melee,
'NonNFT_Exclusive_NEKO_Accessories_NekoSuit_ID2' AS accessories,
'NonNFT_Exclusive_NEKO_Body_NekoSuit_ID2' AS body,
'NonNFT_Exclusive_NEKO_Hands_NekoSuit_ID2' AS hands,
'NonNFT_Exclusive_NEKO_Legs_NekoSuit_ID2' AS legs,
'NonNFT_Exclusive_NEKO_Feet_NekoSuit_ID2' AS feet,
'SipherCharacterSubraceDataAsset:SDA_NEKO_SubraceElemental' AS asset_id

UNION ALL
SELECT
'NEKO.Supernatural' AS id,
'Phasewalker' AS sub_race,
'2' AS ranking,
'NEKO' AS race,
'NEKO_Phasewalker' AS character, 
'DualSMG_Default_ID2' AS gun,
'LightningWhip_Default_ID5' AS melee,
'NonNFT_Exclusive_NEKO_Accessories_NekoSuit_ID2' AS accessories,
'NonNFT_Exclusive_NEKO_Body_NekoSuit_ID2' AS body,
'NonNFT_Exclusive_NEKO_Hands_NekoSuit_ID2' AS hands,
'NonNFT_Exclusive_NEKO_Legs_NekoSuit_ID2' AS legs,
'NonNFT_Exclusive_NEKO_Feet_NekoSuit_ID2' AS feet,
'SipherCharacterSubraceDataAsset:SDA_NEKO_SubraceSupernatural' AS asset_id

UNION ALL
SELECT
'BURU.Normal' AS id,
'Taurus' AS sub_race,
'0' AS ranking,
'BURU' AS race,
'BURU_Tribal' AS character, 
'Shotgun_Default_ID3' AS gun,
'IronFist_Default_ID6' AS melee,
'NonNFT_Exclusive_BURU_Accessories_BuruSuit_ID3' AS accessories,
'NonNFT_Exclusive_BURU_Body_BuruSuit_ID3' AS body,
'NonNFT_Exclusive_BURU_Hands_BuruSuit_ID3' AS hands,
'NonNFT_Exclusive_BURU_Legs_BuruSuit_ID3' AS legs,
'NonNFT_Exclusive_BURU_Feet_BuruSuit_ID3' AS feet,
'SipherCharacterSubraceDataAsset:SDA_BURU_SubraceNormal' AS asset_id

UNION ALL
SELECT
'BURU.Cyborg' AS id,
'Mecha' AS sub_race,
'1' AS ranking,
'BURU' AS race,
'BURU_Mecha' AS character, 
'Shotgun_Default_ID3' AS gun,
'IronFist_Default_ID6' AS melee,
'NonNFT_Exclusive_BURU_Accessories_BuruSuit_ID3' AS accessories,
'NonNFT_Exclusive_BURU_Body_BuruSuit_ID3' AS body,
'NonNFT_Exclusive_BURU_Hands_BuruSuit_ID3' AS hands,
'NonNFT_Exclusive_BURU_Legs_BuruSuit_ID3' AS legs,
'NonNFT_Exclusive_BURU_Feet_BuruSuit_ID3' AS feet,
'SipherCharacterSubraceDataAsset:SDA_BURU_SubraceCyborg' AS asset_id

UNION ALL
SELECT
'BURU.Supernatural' AS id,
'Ancient' AS sub_race,
'3' AS ranking,
'BURU' AS race,
'BURU_Ancient' AS character, 
'Shotgun_Default_ID3' AS gun,
'IronFist_Default_ID6' AS melee,
'NonNFT_Exclusive_BURU_Accessories_BuruSuit_ID3' AS accessories,
'NonNFT_Exclusive_BURU_Body_BuruSuit_ID3' AS body,
'NonNFT_Exclusive_BURU_Hands_BuruSuit_ID3' AS hands,
'NonNFT_Exclusive_BURU_Legs_BuruSuit_ID3' AS legs,
'NonNFT_Exclusive_BURU_Feet_BuruSuit_ID3' AS feet,
'SipherCharacterSubraceDataAsset:SDA_BURU_SubraceSupernatural' AS asset_id

UNION ALL
SELECT
'BURU.Elemental' AS id,
'Inferno' AS sub_race,
'2' AS ranking,
'BURU' AS race,
'BURU_Inferno' AS character, 
'Shotgun_Default_ID3' AS gun,
'IronFist_Default_ID6' AS melee,
'NonNFT_Exclusive_BURU_Accessories_BuruSuit_ID3' AS accessories,
'NonNFT_Exclusive_BURU_Body_BuruSuit_ID3' AS body,
'NonNFT_Exclusive_BURU_Hands_BuruSuit_ID3' AS hands,
'NonNFT_Exclusive_BURU_Legs_BuruSuit_ID3' AS legs,
'NonNFT_Exclusive_BURU_Feet_BuruSuit_ID3' AS feet,
'SipherCharacterSubraceDataAsset:SDA_BURU_SubraceElemental' AS asset_id

-- Sheet: https://docs.google.com/spreadsheets/d/1Fz_uumKc7pl3R-0wxRlw9Vp6Q1TwMztBQmg8JyaeZXg/edit#gid=870188627
