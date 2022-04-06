/* テスト用の設備IDを削除 */
delete from M_MAIL_SEND_EQUIPMENT 
where EQUIPMENT_ID like 'UT_Z_%%'
;
