import logging
import httpx

logger = logging.getLogger(__name__)


async def send_discord_embed(webhook_url: str, title: str, description: str,
                             color: int = 0x2dd4bf, fields: list = None,
                             thumbnail_url: str = None,
                             client: httpx.AsyncClient | None = None):
    if not webhook_url:
        return
    embed = {"title": title, "description": description, "color": color,
             "footer": {"text": "Babel — Media Dub Monitor"}}
    if fields:
        embed["fields"] = fields
    if thumbnail_url:
        embed["thumbnail"] = {"url": thumbnail_url}
    try:
        if client is not None:
            await client.post(webhook_url, json={"embeds": [embed]})
        else:
            async with httpx.AsyncClient(timeout=10) as _client:
                await _client.post(webhook_url, json={"embeds": [embed]})
    except Exception as e:
        logger.warning("Discord notification failed: %s", e)


async def notify_scan_complete(webhook_url: str, stats: dict):
    fields = []
    if stats.get("dubbed"): fields.append({"name": "Dubbed", "value": str(stats["dubbed"]), "inline": True})
    if stats.get("sub_only"): fields.append({"name": "Sub-Only", "value": str(stats["sub_only"]), "inline": True})
    if stats.get("searches_triggered"): fields.append({"name": "Searches", "value": str(stats["searches_triggered"]), "inline": True})
    up_ok = stats.get("upgrades_succeeded", 0)
    up_fail = stats.get("upgrades_failed", 0)
    if up_ok or up_fail:
        fields.append({"name": "Upgrades", "value": f"\u2705 {up_ok} | \u274c {up_fail}", "inline": True})
    color = 0x2dd4bf if up_ok > 0 else 0x3b82f6
    await send_discord_embed(webhook_url, "Scan Complete",
                             f"**{stats.get('episodes_checked', 0)}** episodes checked",
                             color=color, fields=fields)


async def notify_upgrades(webhook_url: str, upgrades: list[dict]):
    if not upgrades:
        return
    by_series = {}
    for u in upgrades:
        t = u.get("series_title", "Unknown")
        if t not in by_series:
            by_series[t] = {"eps": [], "poster": u.get("poster_url")}
        by_series[t]["eps"].append(f"S{u.get('season',0):02d}E{u.get('episode',0):02d}")

    lines = []
    thumb = None
    for title, data in by_series.items():
        eps = ", ".join(data["eps"][:10])
        if len(data["eps"]) > 10: eps += f" (+{len(data['eps'])-10} more)"
        lines.append(f"**{title}** \u2014 {eps}")
        if not thumb: thumb = data.get("poster")

    total = sum(len(d["eps"]) for d in by_series.values())
    await send_discord_embed(webhook_url,
                             f"\U0001f389 {total} Episodes Upgraded to English Dub",
                             "\n".join(lines[:15]),
                             color=0x2dd4bf, thumbnail_url=thumb)
