from __future__ import annotations

from urllib.parse import parse_qs


def _guard_write(deps: dict, *, scope: str) -> str | None:
    guard = deps.get("assert_write_allowed")
    if not callable(guard):
        return None
    try:
        guard(scope=scope, layer="layer1_user_decision")
        return None
    except Exception as exc:
        return str(exc)


def dispatch_get(handler, parsed, deps: dict) -> bool:
    if not parsed.path.startswith("/api/decision"):
        return False

    if parsed.path == "/api/decision/board":
        params = parse_qs(parsed.query)
        try:
            page = int(params.get("page", ["1"])[0] or 1)
            page_size = int(params.get("page_size", ["12"])[0] or 12)
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_decision_board"](
                page=page,
                page_size=page_size,
                ts_code=params.get("ts_code", [""])[0].strip().upper(),
                keyword=params.get("keyword", [""])[0].strip(),
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"决策板查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/today-actions":
        params = parse_qs(parsed.query)
        try:
            limit = int(params.get("limit", ["30"])[0] or 30)
        except ValueError:
            handler._send_json({"ok": False, "error": "limit 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_decision_today_actions"](limit=limit)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"今日动作清单查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/scores":
        params = parse_qs(parsed.query)
        try:
            page_size = int(params.get("page_size", ["8"])[0] or 8)
        except ValueError:
            handler._send_json({"ok": False, "error": "page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_decision_scoreboard"](page_size=page_size)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"评分总览查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/stock":
        params = parse_qs(parsed.query)
        ts_code = params.get("ts_code", [""])[0].strip().upper()
        keyword = params.get("keyword", [""])[0].strip()
        if not ts_code and not keyword:
            handler._send_json({"ok": False, "error": "缺少 ts_code 或 keyword"}, status=400)
            return True
        try:
            payload = deps["query_decision_stock"](ts_code=ts_code, keyword=keyword)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"股票决策查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/plan":
        params = parse_qs(parsed.query)
        try:
            page = int(params.get("page", ["1"])[0] or 1)
            page_size = int(params.get("page_size", ["12"])[0] or 12)
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_decision_trade_plan"](
                page=page,
                page_size=page_size,
                ts_code=params.get("ts_code", [""])[0].strip().upper(),
                keyword=params.get("keyword", [""])[0].strip(),
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"决策板查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/strategies":
        params = parse_qs(parsed.query)
        try:
            page = int(params.get("page", ["1"])[0] or 1)
            page_size = int(params.get("page_size", ["12"])[0] or 12)
            run_id = int(params.get("run_id", ["0"])[0] or 0)
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_decision_strategy_lab"](
                page=page,
                page_size=page_size,
                ts_code=params.get("ts_code", [""])[0].strip().upper(),
                keyword=params.get("keyword", [""])[0].strip(),
                run_id=run_id,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"策略生成查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/strategy-runs":
        params = parse_qs(parsed.query)
        run_id_raw = params.get("run_id", [""])[0].strip()
        try:
            page = int(params.get("page", ["1"])[0] or 1)
            page_size = int(params.get("page_size", ["20"])[0] or 20)
            run_id = int(run_id_raw) if run_id_raw else 0
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            if run_id:
                payload = deps["query_decision_strategy_lab"](
                    page=page,
                    page_size=page_size,
                    ts_code=params.get("ts_code", [""])[0].strip().upper(),
                    keyword=params.get("keyword", [""])[0].strip(),
                    run_id=run_id,
                )
                handler._send_json({"ok": True, **payload})
                return True
            payload = deps["query_decision_strategy_runs"](
                page=page,
                page_size=page_size,
                ts_code=params.get("ts_code", [""])[0].strip().upper(),
                keyword=params.get("keyword", [""])[0].strip(),
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"策略运行历史查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/history":
        params = parse_qs(parsed.query)
        try:
            page = int(params.get("page", ["1"])[0] or 1)
            page_size = int(params.get("page_size", ["20"])[0] or 20)
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_decision_history"](page=page, page_size=page_size)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"历史查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/actions":
        params = parse_qs(parsed.query)
        try:
            page = int(params.get("page", ["1"])[0] or 1)
            page_size = int(params.get("page_size", ["20"])[0] or 20)
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_decision_actions"](
                page=page,
                page_size=page_size,
                ts_code=params.get("ts_code", [""])[0].strip().upper(),
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"动作查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/calibration":
        params = parse_qs(parsed.query)
        try:
            page = int(params.get("page", ["1"])[0] or 1)
            page_size = int(params.get("page_size", ["20"])[0] or 20)
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_decision_calibration"](
                page=page,
                page_size=page_size,
                ts_code=params.get("ts_code", [""])[0].strip().upper(),
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"裁决精准度查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/kill-switch":
        try:
            payload = deps["get_decision_kill_switch"]()
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"状态查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    return False


def dispatch_post(handler, parsed, payload: dict, deps: dict) -> bool:
    if not parsed.path.startswith("/api/decision"):
        return False

    if parsed.path == "/api/decision/trade-advisor/refresh":
        denied = _guard_write(deps, scope="decision.trade_advisor")
        if denied:
            handler._send_json({"ok": False, "error": denied}, status=403)
            return True
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("authenticated"):
            handler._send_json({"ok": False, "error": "请先登录后再重新评估"}, status=401)
            return True
        ts_code = str(payload.get("ts_code", "") or "").strip().upper()
        if not ts_code:
            handler._send_json({"ok": False, "error": "缺少 ts_code"}, status=400)
            return True
        try:
            result = deps["queue_trade_advisor_opinion_refresh"](ts_code=ts_code)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"Agent 重新评估提交失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **result})
        return True

    if parsed.path == "/api/decision/trade-advisor/daily-refresh":
        denied = _guard_write(deps, scope="decision.trade_advisor_daily")
        if denied:
            handler._send_json({"ok": False, "error": denied}, status=403)
            return True
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可触发盘前批量评估"}, status=403)
            return True
        try:
            limit = int(payload.get("limit", 20) or 20)
        except (TypeError, ValueError):
            handler._send_json({"ok": False, "error": "limit 必须是整数"}, status=400)
            return True
        try:
            result = deps["refresh_trade_advisor_daily"](limit=limit)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"盘前 Agent 批量评估失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **result})
        return True

    if parsed.path == "/api/decision/strategy-selection/run":
        denied = _guard_write(deps, scope="decision.strategy_selection")
        if denied:
            handler._send_json({"ok": False, "error": denied}, status=403)
            return True
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可触发策略选股 Agent"}, status=403)
            return True
        try:
            limit = int(payload.get("limit", 30) or 30)
        except (TypeError, ValueError):
            handler._send_json({"ok": False, "error": "limit 必须是整数"}, status=400)
            return True
        try:
            result = deps["run_strategy_selection_agent"](limit=limit)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"策略选股 Agent 运行失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **result})
        return True

    if parsed.path == "/api/decision/kill-switch":
        denied = _guard_write(deps, scope="decision.kill_switch")
        if denied:
            handler._send_json({"ok": False, "error": denied}, status=403)
            return True
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可切换 Kill Switch"}, status=403)
            return True
        allow_trading = payload.get("allow_trading", payload.get("enabled", True))
        reason = str(payload.get("reason", "") or "").strip()
        try:
            result = deps["set_decision_kill_switch"](allow_trading=bool(allow_trading), reason=reason)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"状态更新失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **result})
        return True

    if parsed.path == "/api/decision/snapshot/run":
        denied = _guard_write(deps, scope="decision.snapshot")
        if denied:
            handler._send_json({"ok": False, "error": denied}, status=403)
            return True
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可触发快照"}, status=403)
            return True
        try:
            payload = deps["run_decision_snapshot"]()
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"快照运行失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/decision/strategy-runs/run":
        denied = _guard_write(deps, scope="decision.strategy_runs")
        if denied:
            handler._send_json({"ok": False, "error": denied}, status=403)
            return True
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("authenticated"):
            handler._send_json({"ok": False, "error": "请先登录后再生成策略批次"}, status=401)
            return True
        ts_code = str(payload.get("ts_code", "") or "").strip().upper()
        keyword = str(payload.get("keyword", "") or "").strip()
        try:
            page = int(payload.get("page", 1) or 1)
            page_size = int(payload.get("page_size", 12) or 12)
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            result = deps["run_decision_strategy_generation"](
                page=page,
                page_size=page_size,
                ts_code=ts_code,
                keyword=keyword,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"策略批次生成失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **result})
        return True

    if parsed.path == "/api/decision/actions":
        denied = _guard_write(deps, scope="decision.actions")
        if denied:
            handler._send_json({"ok": False, "error": denied}, status=403)
            return True
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("authenticated"):
            handler._send_json({"ok": False, "error": "请先登录后再记录决策动作"}, status=401)
            return True
        user = auth_ctx.get("user") or {}
        action_type = str(payload.get("action_type", "") or "").strip().lower()
        ts_code = str(payload.get("ts_code", "") or "").strip().upper()
        stock_name = str(payload.get("stock_name", "") or "").strip()
        note = str(payload.get("note", "") or "").strip()
        snapshot_date = str(payload.get("snapshot_date", "") or "").strip()
        context = payload.get("context") if isinstance(payload.get("context"), dict) else {}
        # Structured evidence and review fields — stored in payload JSON blob
        evidence_sources_raw = payload.get("evidence_sources")
        evidence_sources = evidence_sources_raw if isinstance(evidence_sources_raw, list) else []
        evidence_packet = payload.get("evidence_packet") if isinstance(payload.get("evidence_packet"), dict) else {}
        missing_evidence_raw = payload.get("missing_evidence")
        missing_evidence = [str(x) for x in missing_evidence_raw] if isinstance(missing_evidence_raw, list) else []
        evidence_chain_complete_raw = payload.get("evidence_chain_complete")
        evidence_chain_complete = bool(evidence_chain_complete_raw) if evidence_chain_complete_raw is not None else bool(evidence_sources)
        execution_status = str(payload.get("execution_status", "") or "").strip()
        review_conclusion = str(payload.get("review_conclusion", "") or "").strip()
        # P1-5: Idempotency key — prevent duplicate submissions (double-click protection)
        idempotency_key = str(payload.get("idempotency_key", "") or "").strip()
        if not ts_code:
            handler._send_json({"ok": False, "error": "缺少 ts_code"}, status=400)
            return True
        if not action_type:
            handler._send_json({"ok": False, "error": "缺少 action_type"}, status=400)
            return True
        # P0-5: Evidence chain enforcement — warn if key actions lack evidence sources
        evidence_warnings = []
        if action_type in ("confirm", "reject") and not evidence_chain_complete:
            if missing_evidence:
                evidence_warnings.append(f"证据链不完整，缺少：{'、'.join(missing_evidence)}")
            else:
                evidence_warnings.append("此动作缺少完整证据包，建议补充评分与至少一种新闻/信号/群聊证据")
        action_payload: dict = {
            "context": context,
            "source": str(context.get("source") or "decision_board"),
            "evidence_chain_complete": evidence_chain_complete,
        }
        if evidence_sources:
            action_payload["evidence_sources"] = evidence_sources
        if evidence_packet:
            action_payload["evidence_packet"] = evidence_packet
        if missing_evidence:
            action_payload["missing_evidence"] = missing_evidence
        if execution_status:
            action_payload["execution_status"] = execution_status
        if review_conclusion:
            action_payload["review_conclusion"] = review_conclusion
        # Optional action card fields (Section 6.2)
        position_recommendation = str(payload.get("position_recommendation", "") or "").strip()
        expiry_condition = str(payload.get("expiry_condition", "") or "").strip()
        priority = str(payload.get("priority", "") or "medium").strip()
        trigger_reason = str(payload.get("trigger_reason", "") or "").strip()
        if position_recommendation:
            action_payload["position_recommendation"] = position_recommendation
        if expiry_condition:
            action_payload["expiry_condition"] = expiry_condition
        if priority and priority in ("high", "medium", "low"):
            action_payload["priority"] = priority
        if trigger_reason:
            action_payload["trigger_reason"] = trigger_reason
        # Section 6.3: Standardized account-level position fields
        position_pct_range = str(payload.get("position_pct_range", "") or "").strip()
        target_position_pct = payload.get("target_position_pct")
        risk_budget_pct = payload.get("risk_budget_pct")
        if position_pct_range:
            action_payload["position_pct_range"] = position_pct_range
        if target_position_pct is not None:
            try:
                action_payload["target_position_pct"] = float(target_position_pct)
            except (TypeError, ValueError):
                pass
        if risk_budget_pct is not None:
            try:
                action_payload["risk_budget_pct"] = float(risk_budget_pct)
            except (TypeError, ValueError):
                pass
        # Mark whether position info is complete (used by gate logic)
        has_position_info = bool(position_pct_range or action_payload.get("position_recommendation"))
        action_payload["position_info_complete"] = has_position_info
        if action_type in ("confirm",) and not has_position_info:
            evidence_warnings.append("建议填写账户仓位区间（如 5-8%），可执行动作缺少仓位信息将降低执行追溯完整性")
        if idempotency_key:
            action_payload["idempotency_key"] = idempotency_key
            # Idempotency dedup is now handled at the service layer in record_decision_action
            # using a dedicated DB column + unique index — no LIKE query needed here.
        try:
            result = deps["record_decision_action"](
                action_type=action_type,
                ts_code=ts_code,
                stock_name=stock_name,
                note=note,
                actor=str(user.get("username") or user.get("display_name") or "anonymous"),
                snapshot_date=snapshot_date,
                payload=action_payload,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"动作记录失败: {exc}"}, status=500)
            return True
        response = {"ok": True, **result}
        if evidence_warnings:
            response["evidence_warnings"] = evidence_warnings

        # P0-4: Auto-create a non-executable audit task.
        # Executable portfolio orders must be created with explicit size/price later.
        # Use integer id for DB operations; action_id (trace format) is for display only
        _numeric_action_id = result.get("id")  # integer PK
        action_id = str(result.get("action_id") or _numeric_action_id or "")
        if action_type in ("confirm", "reject", "defer", "watch") and ts_code:
            try:
                funnel_sync = deps["sync_decision_action_to_funnel"](
                    action_type=action_type,
                    ts_code=ts_code,
                    stock_name=stock_name,
                    note=note,
                    actor=str(user.get("username") or user.get("display_name") or "anonymous"),
                    snapshot_date=snapshot_date,
                    action_id=action_id,
                )
                response["funnel_sync"] = funnel_sync
            except Exception as _exc:
                # Funnel writeback failure must not block primary decision action.
                response["funnel_sync_warning"] = f"漏斗状态同步失败（不影响决策记录）: {_exc}"

        if action_type in ("confirm", "reject", "defer", "watch") and ts_code and _numeric_action_id:
            try:
                from services.portfolio_service.service import create_order as _create_portfolio_order
                order_action_type = {
                    "confirm": "watch",
                    "reject": "defer",
                    "defer": "defer",
                    "watch": "watch",
                }.get(action_type, action_type)
                order_note_prefix = {
                    "confirm": "[已确认，待手动创建交易计划] ",
                    "reject": "[拒绝记录，不生成交易计划] ",
                    "defer": "[延迟观察] ",
                    "watch": "[跟踪观察] ",
                }.get(action_type, "")
                order_result = _create_portfolio_order(
                    ts_code=ts_code,
                    action_type=order_action_type,
                    planned_price=None,
                    size=0,
                    decision_action_id=str(_numeric_action_id),
                    note=f"{order_note_prefix}{note}".strip(),
                )
                if order_result.get("ok"):
                    response["execution_task"] = {
                        "order_id": order_result.get("id"),
                        "status": "planned",
                        "auto_created": True,
                        "audit_only": True,
                    }
                    # Writeback initial execution_status = "planned" to the decision action
                else:
                    # create_order returned ok=False — surface the failure in the response
                    response["execution_task_warning"] = (
                        f"执行任务自动创建失败: {order_result.get('error', '未知错误')} "
                        f"（决策记录已保存，需手动创建执行任务）"
                    )
                    try:
                        from db_compat import connect as _db_connect
                        import json as _json
                        _conn = _db_connect()
                        try:
                            _row = _conn.execute(
                                "SELECT action_payload_json FROM decision_actions WHERE id = ? LIMIT 1",
                                (_numeric_action_id,),
                            ).fetchone()
                            if _row:
                                _d = dict(_row) if hasattr(_row, 'keys') else {}
                                try:
                                    _payload = _json.loads(str(_d.get("action_payload_json") or "{}"))
                                except Exception:
                                    _payload = {}
                                _payload["execution_status"] = "planned"
                                _conn.execute(
                                    "UPDATE decision_actions SET action_payload_json = ? WHERE id = ?",
                                    (_json.dumps(_payload, ensure_ascii=False), _numeric_action_id),
                                )
                        finally:
                            _conn.close()
                    except Exception:
                        pass  # Writeback failure must not block response
            except Exception as _exc:
                # Auto-link failure must NOT fail the decision action itself
                response["execution_task_warning"] = f"执行任务自动创建失败（不影响决策记录）: {_exc}"

        handler._send_json(response)
        return True

    return False
