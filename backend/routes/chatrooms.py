from __future__ import annotations

from urllib.parse import parse_qs


def dispatch_get(handler, parsed, deps: dict) -> bool:
    if parsed.path == "/api/wechat-chatlog":
        params = parse_qs(parsed.query)
        talker = params.get("talker", [""])[0]
        sender_name = params.get("sender_name", [""])[0]
        keyword = params.get("keyword", [""])[0]
        is_quote = params.get("is_quote", [""])[0]
        query_date_start = params.get("query_date_start", [""])[0]
        query_date_end = params.get("query_date_end", [""])[0]
        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_wechat_chatlog"](
                talker=talker,
                sender_name=sender_name,
                keyword=keyword,
                is_quote=is_quote,
                query_date_start=query_date_start,
                query_date_end=query_date_end,
                page=page,
                page_size=page_size,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/chatrooms":
        params = parse_qs(parsed.query)
        keyword = params.get("keyword", [""])[0]
        primary_category = params.get("primary_category", [""])[0]
        activity_level = params.get("activity_level", [""])[0]
        risk_level = params.get("risk_level", [""])[0]
        skip_realtime_monitor = params.get("skip_realtime_monitor", [""])[0]
        fetch_status = params.get("fetch_status", [""])[0]
        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_chatroom_overview"](
                keyword=keyword,
                primary_category=primary_category,
                activity_level=activity_level,
                risk_level=risk_level,
                skip_realtime_monitor=skip_realtime_monitor,
                fetch_status=fetch_status,
                page=page,
                page_size=page_size,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/chatrooms/fetch":
        params = parse_qs(parsed.query)
        room_id = params.get("room_id", [""])[0]
        mode = params.get("mode", ["today"])[0].strip()
        try:
            payload = deps["fetch_single_chatroom_now"](
                room_id=room_id,
                fetch_yesterday_and_today=(mode == "yesterday_and_today"),
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"立即拉取失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/chatrooms/investment":
        params = parse_qs(parsed.query)
        keyword = params.get("keyword", [""])[0]
        final_bias = params.get("final_bias", [""])[0]
        target_keyword = params.get("target_keyword", [""])[0]
        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_chatroom_investment_analysis"](
                keyword=keyword,
                final_bias=final_bias,
                target_keyword=target_keyword,
                page=page,
                page_size=page_size,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/chatrooms/candidate-pool":
        params = parse_qs(parsed.query)
        keyword = params.get("keyword", [""])[0]
        dominant_bias = params.get("dominant_bias", [""])[0]
        candidate_type = params.get("candidate_type", [""])[0]
        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_chatroom_candidate_pool"](
                keyword=keyword,
                dominant_bias=dominant_bias,
                candidate_type=candidate_type,
                page=page,
                page_size=page_size,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/chatrooms/accuracy":
        params = parse_qs(parsed.query)
        entity_type = params.get("entity_type", [""])[0]
        keyword = params.get("keyword", [""])[0]
        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_chatroom_signal_accuracy"](
                entity_type=entity_type,
                keyword=keyword,
                page=page,
                page_size=page_size,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/chatrooms/room-detail":
        params = parse_qs(parsed.query)
        room_id = params.get("room_id", [""])[0]
        talker = params.get("talker", [""])[0]
        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_chatroom_room_detail"](
                room_id=room_id,
                talker=talker,
                page=page,
                page_size=page_size,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/chatrooms/sender-detail":
        params = parse_qs(parsed.query)
        sender_name = params.get("sender_name", [""])[0]
        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_chatroom_sender_detail"](
                sender_name=sender_name,
                page=page,
                page_size=page_size,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    return False
