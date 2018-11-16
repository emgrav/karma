# karma - A maubot plugin to track the karma of users.
# Copyright (C) 2018 Tulir Asokan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
from typing import Awaitable, Type, Optional
import json
import html

from sqlalchemy.engine.base import Engine

from maubot import Plugin, CommandSpec, Command, PassiveCommand, Argument, MessageEvent
from mautrix.types import (Event, StateEvent, EventID, UserID, FileInfo, MessageType,
                           MediaMessageEventContent)
from mautrix.client.api.types.event.message import media_reply_fallback_body_map
from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper

from .db import make_tables, Karma, Version

COMMAND_PASSIVE_UPVOTE = "xyz.maubot.karma.up"
COMMAND_PASSIVE_DOWNVOTE = "xyz.maubot.karma.down"

ARG_LIST = "$list"
ARG_LIST_MATCHES = "top|bot(?:tom)?|best|worst"
COMMAND_KARMA_LIST = f"karma {ARG_LIST}"
ARG_USER = "$user"
ARG_USER_MATCHES = "@[^:]+:.+"
COMMAND_KARMA_VIEW = f"karma {ARG_USER}"
COMMAND_KARMA_STATS = "karma stats"
COMMAND_OWN_KARMA_VIEW = "karma"
COMMAND_OWN_KARMA_BREAKDOWN = "karma breakdown"
COMMAND_OWN_KARMA_EXPORT = "karma export"

COMMAND_UPVOTE = "upvote"
COMMAND_DOWNVOTE = "downvote"

UPVOTE_EMOJI = r"(?:\U0001F44D[\U0001F3FB-\U0001F3FF]?)"
UPVOTE_EMOJI_SHORTHAND = r"(?:\:\+1\:)|(?:\:thumbsup\:)"
UPVOTE_TEXT = r"(?:\+(?:1|\+)?)"
UPVOTE = f"^(?:{UPVOTE_EMOJI}|{UPVOTE_EMOJI_SHORTHAND}|{UPVOTE_TEXT})$"

DOWNVOTE_EMOJI = r"(?:\U0001F44E[\U0001F3FB-\U0001F3FF]?)"
DOWNVOTE_EMOJI_SHORTHAND = r"(?:\:-1\:)|(?:\:thumbsdown\:)"
DOWNVOTE_TEXT = r"(?:-(?:1|-)?)"
DOWNVOTE = f"^(?:{DOWNVOTE_EMOJI}|{DOWNVOTE_EMOJI_SHORTHAND}|{DOWNVOTE_TEXT})$"


class Config(BaseProxyConfig):
    def do_update(self, helper: ConfigUpdateHelper) -> None:
        helper.copy("democracy")
        helper.copy("filter")


class KarmaBot(Plugin):
    karma: Type[Karma]
    version: Type[Version]
    db: Engine

    async def start(self) -> None:
        self.config.load_and_update()
        self.db = self.request_db_engine()
        self.karma, self.version = make_tables(self.db)
        self.set_command_spec(CommandSpec(commands=[
            Command(syntax=COMMAND_KARMA_STATS, description="View global karma statistics"),
            Command(syntax=COMMAND_OWN_KARMA_VIEW, description="View your karma"),
            Command(syntax=COMMAND_OWN_KARMA_BREAKDOWN, description="View your karma breakdown"),
            Command(syntax=COMMAND_OWN_KARMA_EXPORT, description="Export the data of your karma"),
            Command(syntax=COMMAND_KARMA_LIST, description="View the karma top lists",
                    arguments={ARG_LIST: Argument(matches=ARG_LIST_MATCHES, required=True,
                                                  description="The list to view")}),
            Command(syntax=COMMAND_KARMA_VIEW, description="View the karma of a specific user",
                    arguments={ARG_USER: Argument(matches=ARG_USER_MATCHES, required=True,
                                                  description="The user whose karma to view")}),
            Command(syntax=COMMAND_UPVOTE, description="Upvote a message"),
            Command(syntax=COMMAND_DOWNVOTE, description="Downvote a message"),
        ], passive_commands=[
            PassiveCommand(COMMAND_PASSIVE_UPVOTE, match_against="body", matches=UPVOTE),
            PassiveCommand(COMMAND_PASSIVE_DOWNVOTE, match_against="body", matches=DOWNVOTE)
        ]))

        self.client.add_command_handler(COMMAND_PASSIVE_UPVOTE, self.upvote)
        self.client.add_command_handler(COMMAND_PASSIVE_DOWNVOTE, self.downvote)
        self.client.add_command_handler(COMMAND_UPVOTE, self.upvote)
        self.client.add_command_handler(COMMAND_DOWNVOTE, self.downvote)

        self.client.add_command_handler(COMMAND_KARMA_LIST, self.karma_list)
        self.client.add_command_handler(COMMAND_KARMA_VIEW, self.view_karma)
        self.client.add_command_handler(COMMAND_KARMA_STATS, self.karma_stats)
        self.client.add_command_handler(COMMAND_OWN_KARMA_VIEW, self.view_own_karma)
        self.client.add_command_handler(COMMAND_OWN_KARMA_BREAKDOWN, self.own_karma_breakdown)
        self.client.add_command_handler(COMMAND_OWN_KARMA_EXPORT, self.export_own_karma)

    async def stop(self) -> None:
        self.client.remove_command_handler(COMMAND_PASSIVE_UPVOTE, self.upvote)
        self.client.remove_command_handler(COMMAND_PASSIVE_DOWNVOTE, self.downvote)
        self.client.remove_command_handler(COMMAND_UPVOTE, self.upvote)
        self.client.remove_command_handler(COMMAND_DOWNVOTE, self.downvote)

        self.client.remove_command_handler(COMMAND_KARMA_LIST, self.karma_list)
        self.client.remove_command_handler(COMMAND_KARMA_VIEW, self.view_karma)
        self.client.remove_command_handler(COMMAND_KARMA_STATS, self.karma_stats)
        self.client.remove_command_handler(COMMAND_OWN_KARMA_VIEW, self.view_own_karma)
        self.client.remove_command_handler(COMMAND_OWN_KARMA_BREAKDOWN, self.own_karma_breakdown)
        self.client.remove_command_handler(COMMAND_OWN_KARMA_EXPORT, self.export_own_karma)

    def parse_content(self, evt: Event) -> str:
        if isinstance(evt, MessageEvent):
            if evt.content.msgtype in (MessageType.NOTICE, MessageType.TEXT, MessageType.EMOTE):
                body = evt.content.body
                if evt.content.msgtype == MessageType.EMOTE:
                    body = "/me " + body
                body = body.split("\n")[0]
                if len(body) > 60:
                    body = body[:50] + " \u2026"
                body = html.escape(body)
                return body
            name = media_reply_fallback_body_map[evt.content.msgtype]
            return f"[{name}]({self.client.api.get_download_url(evt.content.url)})"
        elif isinstance(evt, StateEvent):
            return "a state event"
        return "an unknown event"

    @staticmethod
    def sign(value: int) -> str:
        if value > 0:
            return f"+{value}"
        elif value < 0:
            return str(value)
        else:
            return "±0"

    async def vote(self, evt: MessageEvent, target: EventID, value: int) -> None:
        if not target:
            return
        in_filter = evt.sender in self.config["filter"]
        if self.config["democracy"] == in_filter:
            await evt.reply("Sorry, you're not allowed to vote.")
            return
        if self.karma.is_vote_event(target):
            await evt.reply("Sorry, you can't vote on votes.")
            return
        karma_target = await self.client.get_event(evt.room_id, target)
        if not karma_target:
            return
        if karma_target.sender == evt.sender and value > 0:
            await evt.reply("Hey! You can't upvote yourself!")
            return
        karma_id = dict(given_to=karma_target.sender, given_by=evt.sender, given_in=evt.room_id,
                        given_for=karma_target.event_id)
        existing = self.karma.get(**karma_id)
        if existing is not None:
            if existing.value == value:
                await evt.reply(f"You already {self.sign(value)}'d that message.")
                return
            existing.update(new_value=value)
        else:
            karma = self.karma(**karma_id, given_from=evt.event_id, value=value,
                               content=self.parse_content(karma_target))
            karma.insert()
        await evt.mark_read()

    def upvote(self, evt: MessageEvent) -> Awaitable[None]:
        return self.vote(evt, evt.content.get_reply_to(), +1)

    def downvote(self, evt: MessageEvent) -> Awaitable[None]:
        return self.vote(evt, evt.content.get_reply_to(), -1)

    async def karma_stats(self, evt: MessageEvent) -> None:
        await evt.reply("Not yet implemented :(")

    def denotify(self, mxid: UserID) -> str:
        localpart, _ = self.client.parse_mxid(mxid)
        return "\u2063".join(localpart)

    async def karma_list(self, evt: MessageEvent) -> None:
        list_type = evt.content.command.arguments[ARG_LIST]
        if not list_type:
            await evt.reply("**Usage**: !karma [top|bottom|best|worst]")
            return
        message = None
        if list_type in ("top", "bot", "bottom"):
            message = self.karma_user_list(list_type)
        elif list_type in ("best", "worst"):
            message = self.karma_message_list(list_type)
        if message is not None:
            await evt.reply(message)

    def karma_user_list(self, list_type: str) -> Optional[str]:
        if list_type == "top":
            karma_list = self.karma.get_top_users()
            message = "#### Highest karma\n\n"
        elif list_type in ("bot", "bottom"):
            karma_list = self.karma.get_bottom_users()
            message = "#### Lowest karma\n\n"
        else:
            return None
        message += "\n".join(
            f"{index + 1}. [{self.denotify(karma.user_id)}](https://matrix.to/#/{karma.user_id}): "
            f"{self.sign(karma.total)} (+{karma.positive}/-{karma.negative})"
            for index, karma in enumerate(karma_list))
        return message

    def karma_message_list(self, list_type: str) -> Optional[str]:
        if list_type == "best":
            karma_list = self.karma.get_best_events()
            message = "#### Best messages\n\n"
        elif list_type == "worst":
            karma_list = self.karma.get_worst_events()
            message = "#### Worst messages\n\n"
        else:
            return None
        message += "\n".join(
            f"{index + 1}. <a href='https://matrix.to/#/{event.room_id}/{event.event_id}'>Event</a>"
            f" by [{self.denotify(event.sender)}](https://matrix.to/#/{event.sender}) with"
            f" {self.sign(event.total)} karma (+{event.positive}/-{event.negative})\n"
            f"    > {event.content}"
            for index, event in enumerate(karma_list))
        return message

    async def view_karma(self, evt: MessageEvent) -> None:
        try:
            localpart, server_name = self.client.parse_mxid(evt.content.command.arguments[ARG_USER])
        except (ValueError, KeyError):
            return
        mxid = UserID(f"@{localpart}:{server_name}")
        karma = self.karma.get_karma(mxid)
        if karma is None or karma.total is None:
            await evt.reply(f"[{localpart}](https://matrix.to/#/{mxid}) has no karma :(")
            return
        index = self.karma.find_index_from_top(mxid)
        await evt.reply(f"[{localpart}](https://matrix.to/#/{mxid}) has {karma.total} karma "
                        f"(+{karma.positive}/-{karma.negative}) "
                        f"and is #{index + 1 or '∞'} on the top list.")

    async def export_own_karma(self, evt: MessageEvent) -> None:
        karma_list = [karma.to_dict() for karma in self.karma.export(evt.sender)]
        data = json.dumps(karma_list).encode("utf-8")
        url = await self.client.upload_media(data, mime_type="application/json")
        await evt.reply(MediaMessageEventContent(
            msgtype=MessageType.FILE,
            body=f"karma-{evt.sender}.json",
            url=url,
            info=FileInfo(
                mimetype="application/json",
                size=len(data),
            )
        ))

    async def view_own_karma(self, evt: MessageEvent) -> None:
        karma = self.karma.get_karma(evt.sender)
        if karma is None or karma.total is None:
            await evt.reply("You don't have any karma :(")
            return
        index = self.karma.find_index_from_top(evt.sender)
        await evt.reply(f"You have {karma.total} karma (+{karma.positive}/-{karma.negative}) "
                        f"and are #{index + 1 or '∞'} on the top list.")

    async def own_karma_breakdown(self, evt: MessageEvent) -> None:
        await evt.reply("Not yet implemented :(")

    @classmethod
    def get_config_class(cls) -> Type[BaseProxyConfig]:
        return Config
