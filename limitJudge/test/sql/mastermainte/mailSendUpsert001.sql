/* 埋め込み文字列 */
insert
into `M_MAIL_SEND`(
    `MAIL_SEND_SEQ`
    , `MAIL_SEND_ID`
    , `DELETE_FLG`
    , `VERSION`
    , `EMAIL_ADDRESS`
    , `SEND_WEEK_TYPE`
    , `SEND_FREQUANCY`
    , `SEND_TIME_FROM`
    , `SEND_TIME_TO`
    , `MAIL_SUBJECT`
    , `MAIL_TEXT_HEADER`
    , `MAIL_TEXT_BODY`
    , `MAIL_TEXT_FOOTER`
    , `CREATED_AT`
    , `UPDATED_USER`
)
values (
    72
    , '1'
    , '0'
    , 4
    , 'shintaro_otoi@icloud.com'
    , '0'
    , '1'
    , '000000'
    , '235959'
    , '埋め込みサンプル件名'
    , concat(
    			'閾値異常を検知いたしました。\r\n'
    			, '============================='
    		)
    , concat(
		        '【検知日時】@検知日時@\r\n'
		        , '【デバイスID】@デバイスID@\r\n'
		        , '【センサID】@センサID@\r\n'
		        , '【センサ名】@センサ名@\r\n'
		        , '【値】@センサ値@ @単位@\r\n'
		        , '【閾値】@閾値@\r\n'
		        , '【閾値成立回数条件】@閾値成立回数条件@\r\n'
		        , '【閾値成立回数】@閾値成立回数@\r\n'
		        , '【閾値成立回数リセット】@閾値成立回数リセット@\r\n'
		        , '【通知間隔】@通知間隔@\r\n'
		        , '【閾値判定区分】@閾値判定区分@\r\n'
		        , '【ダミー】＠閾値判定区分@\r\n'
		        , '\r\n'
		    )
    , concat(
    			'=============================\r\n'
    			, '※この電子メールは、送信専用メールアドレスよりお送りしています。メールの返信は受け付けておりません。予めご了承下さい。'
    		)
    , CURRENT_TIMESTAMP
    , 'devUser'
)
on DUPLICATE key update
    MAIL_SEND_ID = values(MAIL_SEND_ID)
    ,DELETE_FLG = values(DELETE_FLG)
    ,VERSION = values(VERSION)
    ,EMAIL_ADDRESS = values(EMAIL_ADDRESS)
    ,SEND_WEEK_TYPE = values(SEND_WEEK_TYPE)
    ,SEND_FREQUANCY = values(SEND_FREQUANCY)
    ,SEND_TIME_FROM = values(SEND_TIME_FROM)
    ,SEND_TIME_TO = values(SEND_TIME_TO)
    ,MAIL_SUBJECT = values(MAIL_SUBJECT)
    ,MAIL_TEXT_HEADER = values(MAIL_TEXT_HEADER)
    ,MAIL_TEXT_BODY = values(MAIL_TEXT_BODY)
    ,MAIL_TEXT_FOOTER = values(MAIL_TEXT_FOOTER)
    ,UPDATED_AT = CURRENT_TIMESTAMP
;