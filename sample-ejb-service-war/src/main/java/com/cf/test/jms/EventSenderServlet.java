package com.cf.test.jms;

import javax.ejb.EJB;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/jmstest")
public class EventSenderServlet extends HttpServlet {

    private static final long serialVersionUID = 5339733144881034101L;

    @EJB
    EventSender sender;

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response) {
        final long num = Long.valueOf(request.getParameter("num"));
        final int size = Integer.valueOf(request.getParameter("size"));
        sender.send(num, size);
    }

}
